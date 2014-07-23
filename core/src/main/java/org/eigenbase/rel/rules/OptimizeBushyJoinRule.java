/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.rel.rules;

import java.io.PrintWriter;
import java.util.BitSet;
import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.util.BitSets;

import com.google.common.collect.Lists;

/**
 * Planner rule that finds an approximately optimal ordering for join operators
 * using a heuristic algorithm.
 *
 * <p>It is triggered by the pattern {@link ProjectRel} ({@link MultiJoinRel}).
 *
 * <p>It is similar to {@link org.eigenbase.rel.rules.LoptOptimizeJoinRule}.
 * {@code LoptOptimizeJoinRule} is only capable of producing left-deep joins;
 * this rule is capable of producing bushy joins.
 *
 * <p>TODO:
 * <ol>
 *   <li>Join conditions that touch 1 factor.
 *   <li>Join conditions that touch 3 factors.
 *   <li>More than 1 join conditions that touch the same pair of factors,
 *       e.g. {@code t0.c1 = t1.c1 and t1.c2 = t0.c3}
 * </ol>
 */
public class OptimizeBushyJoinRule extends RelOptRule {
  public static final OptimizeBushyJoinRule INSTANCE =
      new OptimizeBushyJoinRule(RelFactories.DEFAULT_JOIN_FACTORY);

  private final RelFactories.JoinFactory joinFactory;

  /** Creates an OptimizeBushyJoinRule. */
  public OptimizeBushyJoinRule(RelFactories.JoinFactory joinFactory) {
    super(operand(MultiJoinRel.class, any()));
    this.joinFactory = joinFactory;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final MultiJoinRel multiJoinRel = call.rel(0);
    final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);

    final List<Vertex> vertexes = Lists.newArrayList();
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      vertexes.add(new LeafVertex(i));
    }

    final List<LoptMultiJoin.Edge> unusedEdges = Lists.newArrayList();
    for (RexNode node : multiJoin.getJoinFilters()) {
      unusedEdges.add(multiJoin.createEdge(node));
    }

    int groupCount = 0;
    final List<LoptMultiJoin.Edge> usedEdges = Lists.newArrayList();
    while (!unusedEdges.isEmpty()) {
      final int edgeOrdinal =
          chooseBestEdge(unusedEdges);
      if (OptiqPrepareImpl.DEBUG) {
        final PrintWriter pw = new PrintWriter(System.out);
        trace(vertexes, unusedEdges, usedEdges, groupCount, edgeOrdinal, pw);
        pw.flush();
      }
      final LoptMultiJoin.Edge bestEdge = remove(unusedEdges, edgeOrdinal);
      usedEdges.add(bestEdge);

      // For now, assume that the edge is between precisely two factors.
      // 1-factor conditions have probably been pushed down,
      // and 3-or-more-factor conditions are advanced. (TODO:)
      // Therefore, for now, the factors that are merged are exactly the factors
      // on this edge.
      BitSet merged = bestEdge.factors;
      assert merged.cardinality() == 2;
      final int[] factors = BitSets.toArray(merged);

      // Determine which factor is to be on the LHS of the join.
      final int majorFactor;
      final int minorFactor;
      if (vertexes.get(factors[0]).cost <= vertexes.get(factors[1]).cost) {
        majorFactor = factors[0];
        minorFactor = factors[1];
      } else {
        majorFactor = factors[1];
        minorFactor = factors[0];
      }

      // Determine the major vertex.
      final Vertex majorVertex = vertexes.get(majorFactor);
      final Vertex minorVertex = vertexes.get(majorFactor);

      int newGroup;
      if (majorVertex.group == minorVertex.group) {
        // Vertexes must be unassigned.
        // (If they were assigned to the same group, we should have added all
        // edge when they were assigned.)
        assert majorVertex.group == -1;
        newGroup = groupCount++;
      } else {
        final int minorGroup = minorVertex.group;
        final int majorGroup = majorVertex.group;

        // Convert all vertexes in the minor vertex's group to the major group.
        for (Vertex vertex2 : vertexes) {
          if (vertex2.group == minorGroup) {
            vertex2.group = majorGroup;
          }
        }

        newGroup = majorGroup;
      }

      final int v = vertexes.size();
      final Vertex newVertex =
          new JoinVertex(v, newGroup, majorFactor, minorFactor,
              BitSets.union(majorVertex.factors, minorVertex.factors));
      vertexes.add(newVertex);

      // Re-compute selectivity of edges above the one just chosen.
      // Suppose that we just chose the edge between "product" (10k rows) and
      // "product_class" (10 rows).
      // Both of those vertices are now replaced by a new vertex "P-PC".
      // This vertex has fewer rows (1k rows) -- a fact that is critical to
      // decisions made later. (Hence "greedy" algorithm not "simple".)
      // The adjacent edges are modified.
      for (int i = 0; i < unusedEdges.size(); i++) {
        final LoptMultiJoin.Edge edge = unusedEdges.get(i);
        final LoptMultiJoin.Edge newEdge;
        if (edge.factors.intersects(merged)) {
          BitSet newFactors = (BitSet) edge.factors.clone();
          newFactors.andNot(merged);
          newFactors.set(v);
          newEdge =
              new LoptMultiJoin.Edge(edge.condition, newFactors, edge.columns);
          unusedEdges.set(i, newEdge);
        }
      }
    }
  }

  private void trace(List<Vertex> vertexes,
      List<LoptMultiJoin.Edge> unusedEdges, List<LoptMultiJoin.Edge> usedEdges,
      int groupCount, int edgeOrdinal, PrintWriter pw) {
    pw.println("groupCount: " + groupCount);
    pw.println("bestEdge: " + edgeOrdinal);
    pw.println("vertexes:");
    for (Vertex vertex : vertexes) {
      pw.println(vertex);
    }
    pw.println("unused edges:");
    for (LoptMultiJoin.Edge edge : unusedEdges) {
      pw.println(edge);
    }
    pw.println("edges:");
    for (LoptMultiJoin.Edge edge : usedEdges) {
      pw.println(edge);
    }
    pw.println();
  }

  /** Removes the element of a list at a given ordinal, moving the last element
   * into its place. This is an efficient means of removing an element from an
   * array list if you do not mind the order of the list changing. */
  private static <E> E remove(List<E> list, int ordinal) {
    final int lastOrdinal = list.size() - 1;
    final E last = list.remove(lastOrdinal);
    if (ordinal == lastOrdinal) {
      return last;
    }
    final E e = list.get(ordinal);
    list.set(ordinal, last);
    return e;
  }

  int chooseBestEdge(List<LoptMultiJoin.Edge> edges) {
    return 0;
  }

  /** Participant in a join (relation or join). */
  abstract static class Vertex {
    final int id;

    /** Ordinal of the equivalence set that this vertex belongs to.
     *
     * <p>The initial vertexes all belong to group 0.
     *
     * <p>If you combine two vertexes in group 0, the new vertex is in a new
     * group.
     *
     * <p>If you combine two vertexes neither of which is in group 0, we need to
     * choose between the two groups, and we choose the one with fewer rows. The
     * other vertex, and all vertexes in its group, are assigned to that group.
     */
    protected int group;
    protected final BitSet factors;
    public double cost;

    Vertex(int id, int group, BitSet factors) {
      this.id = id;
      this.group = group;
      this.factors = factors;
    }
  }

  /** Relation participating in a join. */
  static class LeafVertex extends Vertex {
    LeafVertex(int id) {
      super(id, -1, BitSets.of(id));
    }

    @Override public String toString() {
      return "LeafVertex(id: " + id + ", group: " + group
          + ", factors: " + factors + ")";
    }
  }

  /** Participant in a join which is itself a join. */
  static class JoinVertex extends Vertex {
    private final int leftFactor;
    private final int rightFactor;

    JoinVertex(int id, int group, int leftFactor, int rightFactor,
        BitSet factors) {
      super(id, group, factors);
      this.leftFactor = leftFactor;
      this.rightFactor = rightFactor;
    }

    @Override public String toString() {
      return "JoinVertex(id: " + id + ", group: " + group
          + ", factors: " + factors
          + ", leftFactor: " + leftFactor
          + ", rightFactor: " + rightFactor
          + ")";
    }
  }
}

// End OptimizeBushyJoinRule.java
