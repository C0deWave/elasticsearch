/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;

import java.util.HashMap;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class UpdateNumberOfReplicasTests extends ESAllocationTestCase {

    public void testAutoExpandRetainsReplicasOnRemainingNodes() {
        assertAutoExpandRetainsReplicasOnRemainingNodes(1);
    }

    public void testAutoExpandRetainsReplicasWhenRemovingMultipleNodes() {
        assertAutoExpandRetainsReplicasOnRemainingNodes(between(2, 4));
    }

    private void assertAutoExpandRetainsReplicasOnRemainingNodes(int removingNodeCount) {
        final var indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all"))
            .build();
        final int nodeCount = removingNodeCount + between(2, 5);
        final var nodes = DiscoveryNodes.builder();
        for (int i = 0; i < nodeCount; i++) {
            nodes.add(newNode("node-" + i));
        }
        final var allocationService = createAllocationService();
        var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodes)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata))
            .build();
        state = applyStartedShardsUntilNoChange(state, allocationService);
        final var originalShards = state.routingTable().index("test").shard(0);
        assertEquals(nodeCount, originalShards.size());
        assertTrue(originalShards.allShardsStarted());

        // Keep the first replica in routing order: removing it used to trigger an unnecessary recovery on its node.
        final var shutdowns = new HashMap<String, SingleNodeShutdownMetadata>();
        final var replicas = originalShards.replicaShards();
        for (int i = replicas.size() - removingNodeCount; i < replicas.size(); i++) {
            final var nodeId = replicas.get(i).currentNodeId();
            final var type = randomFrom(SingleNodeShutdownMetadata.Type.REMOVE, SingleNodeShutdownMetadata.Type.SIGTERM);
            shutdowns.put(
                nodeId,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(nodeId)
                    .setNodeEphemeralId(state.nodes().get(nodeId).getEphemeralId())
                    .setType(type)
                    .setReason("test")
                    .setStartedAtMillis(1L)
                    .setGracePeriod(type == SingleNodeShutdownMetadata.Type.SIGTERM ? TimeValue.timeValueMinutes(1) : null)
                    .build()
            );
        }
        // A restart must not cause the retained replica to be preferred for removal.
        final var restartingNode = replicas.get(0).currentNodeId();
        shutdowns.put(
            restartingNode,
            SingleNodeShutdownMetadata.builder()
                .setNodeId(restartingNode)
                .setNodeEphemeralId(state.nodes().get(restartingNode).getEphemeralId())
                .setType(SingleNodeShutdownMetadata.Type.RESTART)
                .setReason("test")
                .setStartedAtMillis(1L)
                .build()
        );
        final var shutdownMetadata = new NodesShutdownMetadata(shutdowns);
        state = ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata))
            .build();
        state = allocationService.reroute(state, "register node shutdown", ActionListener.noop());

        assertEquals(nodeCount - removingNodeCount - 1, state.metadata().getProject().index("test").getNumberOfReplicas());
        final var remainingShards = state.routingTable().index("test").shard(0);
        assertTrue(remainingShards.toString(), remainingShards.allShardsStarted());
        for (int i = 0; i < originalShards.size(); i++) {
            final var originalShard = originalShards.shard(i);
            final var currentShard = state.getRoutingNodes().node(originalShard.currentNodeId()).getByShardId(originalShard.shardId());
            if (shutdownMetadata.isNodeMarkedForRemoval(originalShard.currentNodeId())) {
                assertNull(currentShard);
            } else {
                assertNotNull(currentShard);
                assertEquals(originalShard.allocationId(), currentShard.allocationId());
                assertEquals(STARTED, currentShard.state());
            }
        }
    }

    public void testUpdateNumberOfReplicas() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(initialRoutingTable.index("test").size(), equalTo(1));
        assertThat(initialRoutingTable.index("test").shard(0).size(), equalTo(2));
        assertThat(initialRoutingTable.index("test").shard(0).size(), equalTo(2));
        assertThat(initialRoutingTable.index("test").shard(0).shard(0).state(), equalTo(UNASSIGNED));
        assertThat(initialRoutingTable.index("test").shard(0).shard(1).state(), equalTo(UNASSIGNED));
        assertThat(initialRoutingTable.index("test").shard(0).shard(0).currentNodeId(), nullValue());
        assertThat(initialRoutingTable.index("test").shard(0).shard(1).currentNodeId(), nullValue());

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("Start all the primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("Start all the replica shards");
        ClusterState newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        final String nodeHoldingPrimary = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        final String nodeHoldingReplica = clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId();
        assertThat(nodeHoldingPrimary, not(equalTo(nodeHoldingReplica)));
        assertThat(clusterState.routingTable().index("test").size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(), equalTo(nodeHoldingReplica));

        logger.info("add another replica");
        final String[] indices = { "test" };
        RoutingTable updatedRoutingTable = RoutingTable.builder(
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            clusterState.routingTable()
        ).updateNumberOfReplicas(2, indices).build();
        metadata = Metadata.builder(clusterState.metadata()).updateNumberOfReplicas(2, indices).build();
        clusterState = ClusterState.builder(clusterState).routingTable(updatedRoutingTable).metadata(metadata).build();

        assertThat(clusterState.metadata().getProject().index("test").getNumberOfReplicas(), equalTo(2));

        assertThat(clusterState.routingTable().index("test").size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(3));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(), equalTo(nodeHoldingReplica));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(1).state(), equalTo(UNASSIGNED));

        logger.info("Add another node and start the added replica");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(3));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).size(), equalTo(1));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).get(0).currentNodeId(),
            equalTo(nodeHoldingReplica)
        );
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShardsWithState(INITIALIZING).get(0).currentNodeId(),
            equalTo("node3")
        );

        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(3));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).size(), equalTo(2));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).get(0).currentNodeId(),
            anyOf(equalTo(nodeHoldingReplica), equalTo("node3"))
        );
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).get(1).currentNodeId(),
            anyOf(equalTo(nodeHoldingReplica), equalTo("node3"))
        );

        logger.info("now remove a replica");
        updatedRoutingTable = RoutingTable.builder(clusterState.routingTable()).updateNumberOfReplicas(1, indices).build();
        metadata = Metadata.builder(clusterState.metadata()).updateNumberOfReplicas(1, indices).build();
        clusterState = ClusterState.builder(clusterState).routingTable(updatedRoutingTable).metadata(metadata).build();

        assertThat(clusterState.metadata().getProject().index("test").getNumberOfReplicas(), equalTo(1));

        assertThat(clusterState.routingTable().index("test").size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(STARTED));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(),
            anyOf(equalTo(nodeHoldingReplica), equalTo("node3"))
        );

        logger.info("do a reroute, should remain the same");
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));
    }
}
