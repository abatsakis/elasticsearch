/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.isIntegral;

/**
 * Removes {@code STATS BY} keys that do not add grouping cardinality and rebuilds their output above the aggregation. When it
 * prunes, it also drops fields of the {@code EVAL} directly below the aggregate that nothing reads any more.
 * <p>
 * A scalar constant qualifies unconditionally: {@code STATS ... BY 1, x} or {@code EVAL c = 1 | STATS ... BY c, x} group on
 * {@code x} alone and re-emit the constant in an {@link Eval} above the {@link Aggregate}. A constant is single-valued on every
 * row, so this is always equivalent.
 * <p>
 * A key derived from other keys with {@code +}, {@code -} or unary minus (for example
 * {@code EVAL m = x - 1 | STATS ... BY x, m}) is equivalent to grouping on {@code x} and recomputing {@code m} per group only
 * while every input is single-valued. On a row where {@code x} is a list, the query as written evaluates {@code x - 1} to
 * {@code null} (with a warning), while the aggregate unrolls the list into one group per element. Recomputing {@code m} per
 * group would then produce a number where the query returns {@code null}, and the group set itself can differ. The rule
 * therefore prunes a derived key only when complete source statistics prove each referenced column has at most one value per
 * row:
 * <ul>
 *   <li>a known {@code value_count} equal to {@code row_count - null_count} (every format; a multivalued cell contributes one
 *       per value, so any list pushes the count above the non-null rows), or</li>
 *   <li>for Parquet, a known {@code null_count} and no {@code value_count}. Parquet publishes a null count only for a
 *       non-repeated leaf, which holds at most one value per row, and withholds it for a list.</li>
 * </ul>
 * Partial statistics (an anchor file standing in for a multi-file dataset) prove nothing. An index field carries no such
 * proof, so a derived key over an index stays in the aggregate.
 */
public final class PruneRedundantAggregateGroupings extends OptimizerRules.OptimizerRule<Aggregate>
    implements
        OptimizerRules.LocalAware<Aggregate> {

    /**
     * Alias hops expanded while unfolding a derived key. Deeper than this, the rule keeps the key: a chain that long is not
     * worth the recursion, and giving up is always a correct plan.
     */
    private static final int MAX_ALIAS_EXPANSION_DEPTH = 32;

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        if (shouldSkipAggregate(aggregate)) {
            return aggregate;
        }

        AttributeMap<Expression> evalAliases = evalAliases(aggregate.child());
        AttributeSet retainedGroupingAttributes = retainedGroupingAttributes(aggregate.groupings(), evalAliases);
        AttributeSet singleValuedAttributes = singleValuedAttributes(aggregate.child());
        // A grouping key may be re-exposed under a different name in the aggregate output, e.g. a renamed column
        // `... | RENAME x AS y | STATS ... BY y` surfaces as `x AS y` in the aggregate's output. A pruned grouping is
        // rebuilt as an Eval above the aggregate, so its expression must read the aggregate's output attribute (`y`),
        // not the pre-aggregate attribute (`x`) which the aggregate no longer surfaces.
        AttributeMap<Attribute> groupingOutputAttributes = groupingOutputAttributes(aggregate.aggregates());
        List<Expression> newGroupings = new ArrayList<>(aggregate.groupings().size());
        List<PrunedGrouping> prunedGroupings = new ArrayList<>();

        for (Expression grouping : aggregate.groupings()) {
            Expression replacement = replacementFor(
                grouping,
                evalAliases,
                retainedGroupingAttributes,
                singleValuedAttributes,
                groupingOutputAttributes
            );
            if (replacement == null) {
                newGroupings.add(grouping);
            } else {
                prunedGroupings.add(new PrunedGrouping(grouping, replacement));
            }
        }

        if (prunedGroupings.isEmpty() || newGroupings.isEmpty()) {
            return aggregate;
        }

        List<NamedExpression> newAggregates = new ArrayList<>(aggregate.aggregates().size());
        List<Alias> postAggregateEvals = new ArrayList<>();
        for (NamedExpression aggregateExpression : aggregate.aggregates()) {
            PrunedGrouping pruned = matchingPrunedGrouping(aggregateExpression, prunedGroupings);
            if (pruned == null) {
                newAggregates.add(aggregateExpression);
            } else {
                postAggregateEvals.add(reconstruct(aggregateExpression, pruned.replacement()));
            }
        }

        LogicalPlan plan = aggregate.with(
            pruneUnusedChildEvals(aggregate.child(), newGroupings, newAggregates),
            newGroupings,
            newAggregates
        );
        if (postAggregateEvals.isEmpty() == false) {
            plan = new Eval(aggregate.source(), plan, postAggregateEvals);
            plan = new Project(aggregate.source(), plan, aggregate.aggregates().stream().map(NamedExpression::toAttribute).toList());
        }
        return plan;
    }

    @Override
    public Rule<Aggregate, LogicalPlan> local() {
        return null;
    }

    private static boolean shouldSkipAggregate(Aggregate aggregate) {
        // Inline stats RHS aggregates use StubRelation while their join keys still mirror the original groupings.
        return aggregate.groupings().isEmpty()
            || aggregate instanceof TimeSeriesAggregate
            || aggregate.child().anyMatch(StubRelation.class::isInstance);
    }

    private static AttributeMap<Expression> evalAliases(LogicalPlan plan) {
        AttributeMap.Builder<Expression> aliases = AttributeMap.builder();
        plan.forEachDown(Eval.class, eval -> eval.fields().forEach(alias -> aliases.put(alias.toAttribute(), alias.child())));
        return aliases.build();
    }

    private static AttributeSet retainedGroupingAttributes(List<Expression> groupings, AttributeMap<Expression> evalAliases) {
        AttributeSet.Builder retained = AttributeSet.builder();
        for (Expression grouping : groupings) {
            Attribute attribute = Expressions.attribute(grouping);
            if (attribute != null && evalAliases.containsKey(attribute) == false) {
                retained.add(attribute);
            }
        }
        return retained.build();
    }

    /**
     * Maps each attribute that the aggregate's output is built from to the attribute that actually exposes it. A direct
     * pass-through (e.g. {@code BY x} emitting {@code x}) maps to itself; a rename (e.g. {@code x AS y}) maps the
     * underlying attribute {@code x} to the output attribute {@code y}. This lets a pruned grouping be rebuilt above the
     * aggregate while reading the aggregate's output rather than a pre-aggregate attribute the aggregate no longer
     * surfaces.
     */
    private static AttributeMap<Attribute> groupingOutputAttributes(List<? extends NamedExpression> aggregates) {
        AttributeMap.Builder<Attribute> outputAttributes = AttributeMap.builder();
        for (NamedExpression aggregate : aggregates) {
            // A direct pass-through (e.g. `BY x` emitting `x`) takes precedence over a rename of the same underlying
            // attribute: put() always wins for the identity case, while computeIfAbsent() keeps an alias only when no
            // pass-through has claimed the key.
            if (aggregate instanceof Attribute attribute) {
                outputAttributes.put(attribute, attribute);
            } else if (aggregate instanceof Alias alias && alias.child() instanceof Attribute attribute) {
                outputAttributes.computeIfAbsent(attribute, key -> alias.toAttribute());
            }
        }
        return outputAttributes.build();
    }

    /**
     * Columns whose complete source statistics show at most one value per row. See the class javadoc for which
     * statistics count as that proof. A relation marked {@code _stats.partial} contributes nothing: its numbers describe
     * an anchor file, not the dataset the aggregate reads.
     */
    private static AttributeSet singleValuedAttributes(LogicalPlan plan) {
        AttributeSet.Builder proven = AttributeSet.builder();
        plan.forEachDown(ExternalRelation.class, relation -> addSingleValuedAttributes(relation, proven));
        return proven.build();
    }

    private static void addSingleValuedAttributes(ExternalRelation relation, AttributeSet.Builder proven) {
        Map<String, Object> sourceMetadata = relation.metadata().sourceMetadata();
        if (sourceMetadata != null && Boolean.TRUE.equals(sourceMetadata.get(SourceStatisticsSerializer.STATS_PARTIAL))) {
            return;
        }
        SourceStatistics statistics = SourceStatisticsSerializer.fromSource(relation.metadata());
        if (statistics == null || statistics.rowCount().isEmpty()) {
            return;
        }
        long rows = statistics.rowCount().getAsLong();
        Map<String, SourceStatistics.ColumnStatistics> columns = statistics.columnStatistics().orElse(Map.of());
        if (columns.isEmpty() || rows < 0) {
            return;
        }
        boolean nullCountProvesSingleValue = FormatNameResolver.FORMAT_PARQUET.equals(relation.sourceType());
        Map<String, Attribute> byName = new HashMap<>();
        Set<String> ambiguous = new HashSet<>();
        for (Attribute attribute : relation.output()) {
            Attribute previous = byName.put(attribute.name(), attribute);
            if (previous != null) {
                ambiguous.add(attribute.name());
            }
        }
        for (Map.Entry<String, SourceStatistics.ColumnStatistics> entry : columns.entrySet()) {
            if (ambiguous.contains(entry.getKey())) {
                continue;
            }
            Attribute attribute = byName.get(entry.getKey());
            if (attribute != null && atMostOneValuePerRow(entry.getValue(), rows, nullCountProvesSingleValue)) {
                proven.add(attribute);
            }
        }
    }

    /**
     * {@code true} when {@code column} has exactly one value on every non-null row and none on a null row.
     * {@code valueCount}, when known, is that statement directly. When it is absent, only a Parquet null count stands
     * in: Parquet records one for a non-repeated leaf and withholds it for a list, so a present count is the proof.
     */
    private static boolean atMostOneValuePerRow(SourceStatistics.ColumnStatistics column, long rows, boolean nullCountProvesSingleValue) {
        if (column.nullCount().isEmpty()) {
            return false;
        }
        long nulls = column.nullCount().getAsLong();
        if (nulls < 0 || nulls > rows) {
            return false;
        }
        long nonNullRows = rows - nulls;
        if (column.valueCount().isPresent()) {
            return column.valueCount().getAsLong() == nonNullRows;
        }
        return nullCountProvesSingleValue;
    }

    private static LogicalPlan pruneUnusedChildEvals(
        LogicalPlan child,
        List<Expression> newGroupings,
        List<NamedExpression> newAggregates
    ) {
        if (!(child instanceof Eval eval)) {
            return child;
        }

        // Only the aggregate reads this Eval, so a field is needed only if the aggregate or a needed field after it reads it,
        // such as a kept grouping `b = a * 2` reading a pruned `a`. A field reads only fields before it, so walking backwards
        // settles each field after all of its readers.
        AttributeSet.Builder required = Aggregate.computeReferences(newAggregates, newGroupings).asBuilder();
        List<Alias> fields = eval.fields();
        List<Alias> remainingFields = new ArrayList<>(fields.size());
        for (int i = fields.size() - 1; i >= 0; i--) {
            Alias field = fields.get(i);
            if (required.contains(field.toAttribute())) {
                required.addAll(field.child().references());
                remainingFields.add(field);
            }
        }
        if (remainingFields.size() == fields.size()) {
            return child;
        }
        Collections.reverse(remainingFields);
        return remainingFields.isEmpty() ? eval.child() : new Eval(eval.source(), eval.child(), remainingFields);
    }

    /**
     * The expression to re-emit above the aggregate in place of {@code grouping}, or {@code null} if the grouping must
     * stay. A scalar constant, written inline or through an {@link Eval} alias, can always be pruned. A key derived with
     * {@code +}, {@code -} or unary minus can be pruned when every column it reads is {@linkplain #singleValuedAttributes
     * proven single-valued} and is itself a grouping the aggregate still exposes.
     */
    private static Expression replacementFor(
        Expression grouping,
        AttributeMap<Expression> evalAliases,
        AttributeSet retainedGroupingAttributes,
        AttributeSet singleValuedAttributes,
        AttributeMap<Attribute> groupingOutputAttributes
    ) {
        Expression unwrapped = Alias.unwrap(grouping);
        if (isScalarFoldable(unwrapped)) {
            return unwrapped;
        }

        Attribute groupingAttribute = Expressions.attribute(grouping);
        if (groupingAttribute == null) {
            return null;
        }

        Expression definition = evalAliases.get(groupingAttribute);
        if (definition == null) {
            return null;
        }
        if (isScalarFoldable(definition)) {
            return definition;
        }
        if (singleValuedAttributes.isEmpty()) {
            return null;
        }

        Expression expanded = expandAliases(definition, evalAliases, retainedGroupingAttributes);
        if (expanded != null
            && isSafeDerivedExpression(expanded, retainedGroupingAttributes, singleValuedAttributes, groupingOutputAttributes)) {
            // The expression references the retained grouping attributes as they exist below the aggregate; rebind them
            // to the attributes the aggregate exposes so the rebuilt Eval above the aggregate stays consistent.
            return expanded.transformUp(Attribute.class, attribute -> groupingOutputAttributes.resolve(attribute, attribute));
        }
        return null;
    }

    /**
     * Unfolds {@code EVAL} aliases one hop at a time, stopping at a retained grouping. Returns {@code null} when the
     * chain is deeper than {@link #MAX_ALIAS_EXPANSION_DEPTH} or cycles, so the caller keeps the grouping.
     */
    private static Expression expandAliases(
        Expression expression,
        AttributeMap<Expression> evalAliases,
        AttributeSet retainedGroupingAttributes
    ) {
        Expression current = expression;
        for (int depth = 0; depth <= MAX_ALIAS_EXPANSION_DEPTH; depth++) {
            boolean expands = false;
            for (Attribute reference : current.references()) {
                if (retainedGroupingAttributes.contains(reference) == false && evalAliases.containsKey(reference)) {
                    expands = true;
                    break;
                }
            }
            if (expands == false) {
                return current;
            }
            if (depth == MAX_ALIAS_EXPANSION_DEPTH) {
                return null;
            }
            current = current.transformUp(Attribute.class, attribute -> {
                if (retainedGroupingAttributes.contains(attribute)) {
                    return attribute;
                }
                Expression replacement = evalAliases.get(attribute);
                return replacement == null ? attribute : replacement;
            });
        }
        return null;
    }

    private static boolean isSafeDerivedExpression(
        Expression expression,
        AttributeSet retainedGroupingAttributes,
        AttributeSet singleValuedAttributes,
        AttributeMap<Attribute> groupingOutputAttributes
    ) {
        AttributeSet references = expression.references();
        // The containsKey check requires every referenced attribute to be exposed by the aggregate output, otherwise the
        // rebuilt Eval above the aggregate would dangle on an attribute the aggregate no longer surfaces.
        return references.isEmpty() == false
            && references.subsetOf(retainedGroupingAttributes)
            && references.subsetOf(singleValuedAttributes)
            && references.stream().allMatch(groupingOutputAttributes::containsKey)
            && references.stream().allMatch(PruneRedundantAggregateGroupings::isSafeIntegralAttribute)
            && expression.anyMatch(PruneRedundantAggregateGroupings::isUnsafeDerivedExpression) == false;
    }

    private static boolean isUnsafeDerivedExpression(Expression expression) {
        if (expression instanceof Attribute) {
            return false;
        }
        if (isScalarFoldable(expression)) {
            return false;
        }
        return expression instanceof Add == false && expression instanceof Sub == false && expression instanceof Neg == false;
    }

    private static boolean isSafeIntegralAttribute(Attribute attribute) {
        return isIntegral(attribute.dataType());
    }

    private static boolean isScalarFoldable(Expression expression) {
        if (expression.foldable() == false) {
            return false;
        }
        return expression.fold(FoldContext.small()) instanceof List<?> == false;
    }

    private static PrunedGrouping matchingPrunedGrouping(NamedExpression aggregateExpression, List<PrunedGrouping> prunedGroupings) {
        Attribute aggregateAttribute = aggregateExpression.toAttribute();
        Expression aggregateChild = Alias.unwrap(aggregateExpression);
        for (PrunedGrouping prunedGrouping : prunedGroupings) {
            Attribute groupingAttribute = Expressions.attribute(prunedGrouping.grouping());
            if ((groupingAttribute != null && aggregateAttribute.semanticEquals(groupingAttribute))
                || aggregateChild.semanticEquals(Alias.unwrap(prunedGrouping.grouping()))) {
                return prunedGrouping;
            }
        }
        return null;
    }

    private static Alias reconstruct(NamedExpression aggregateExpression, Expression replacement) {
        if (aggregateExpression instanceof Alias alias) {
            return alias.replaceChild(replacement);
        }
        return new Alias(
            aggregateExpression.source(),
            aggregateExpression.name(),
            replacement,
            aggregateExpression.id(),
            aggregateExpression.synthetic()
        );
    }

    private record PrunedGrouping(Expression grouping, Expression replacement) {}
}
