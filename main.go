package main

import (
	"log"
	"sync"
)

func main() {
	tree := initTree(17, "hello17")
	for i := 0; i < 100; i++ {
		tree.insert(100-i, "hello")
	}
	tree.printTree()
}

type palmTree struct {
	Degree         int
	Root           *node
	stageWaitGroup sync.WaitGroup
}

func initTree(key int, firstElem string) *palmTree {
	rootNode := node{
		IsLeafType: true,
		IsRootType: true,
		Count:      1,
	}
	rootNode.NodeKey = &nodeKey{Key: key, Value: firstElem}
	tree := &palmTree{Degree: 5, Root: &rootNode}
	return tree
}

func (tree *palmTree) printTree() {
	var currentNodeKeyPointer = tree.Root.NodeKey
	for currentNodeKeyPointer != nil {
		log.Println(currentNodeKeyPointer.Key)
		currentNodeKeyPointer = currentNodeKeyPointer.NextKey
	}
}

func (tree *palmTree) palm(goroutineID int, goroutineOperations []Operation, operationsToNodes *sync.Map) {
	//first stage - search nodes
	tree.stageWaitGroup.Add(1)
	goroutineOperationsToNodes := make(map[Operation]*node)
	for _, op := range goroutineOperations {
		_, leafNode := tree.search(op.Key)
		operationsToNodes.Store(op, leafNode)
		goroutineOperationsToNodes[op] = leafNode
	}
	tree.stageWaitGroup.Done()
	tree.stageWaitGroup.Wait()
	//second stage - get work for current goroutine
	tree.stageWaitGroup.Add(1)
	newGoroutineOperationsToNodes := make(map[Operation]*node)
	for op, leafNode := range goroutineOperationsToNodes {
		newGoroutineOperationsToNodes[op] = leafNode
	}
	tree.stageWaitGroup.Done()
	tree.stageWaitGroup.Wait()
	//end second stage
}

func (tree *palmTree) insert(key int, value string) {
	_, leafNode := tree.search(key)
	var currentNodeKeyPointer = leafNode.NodeKey
	var previousNodeKeyPointer *nodeKey
	for currentNodeKeyPointer != nil && currentNodeKeyPointer.Key <= key {
		previousNodeKeyPointer = currentNodeKeyPointer
		currentNodeKeyPointer = currentNodeKeyPointer.NextKey
	}
	newKey := nodeKey{Key: key, Value: value}
	newKey.NextKey = currentNodeKeyPointer
	if previousNodeKeyPointer != nil {
		previousNodeKeyPointer.NextKey = &newKey
	} else {
		leafNode.NodeKey = &newKey
	}
	leafNode.Count++
}

func (tree *palmTree) delete(key int) {
	_, leafNode := tree.search(key)
	var nodeKeyPointer = leafNode.NodeKey
	var previousNodeKeyPointer *nodeKey
	for nodeKeyPointer != nil && nodeKeyPointer.Key != key {
		previousNodeKeyPointer = nodeKeyPointer
		nodeKeyPointer = nodeKeyPointer.NextKey
	}
	if nodeKeyPointer != nil {
		previousNodeKeyPointer.NextKey = nodeKeyPointer.NextKey
	}
	leafNode.Count--
}

func (tree *palmTree) search(key int) (string, *node) {
	return tree.searchRec(key, tree.Root)
}

func (tree *palmTree) searchRec(key int, node *node) (string, *node) {
	if node.IsLeafType {
		var nodeKey = node.NodeKey
		for nodeKey != nil && nodeKey.Key != key {
			nodeKey = nodeKey.NextKey
		}
		if nodeKey == nil {
			return "", node
		}
		return nodeKey.Value, node
	}
	var nodeLink = node.NodeLink
	for nodeLink != nil && nodeLink.NextNodeKey != nil && nodeLink.NextNodeKey.Key <= key {
		nodeLink = nodeLink.NextNodeKey.NextNodeLink
	}
	return tree.searchRec(key, nodeLink.LinkValue)
}

//internal node region
type node struct {
	IsLeafType bool
	IsRootType bool
	NodeLink   *nodeLink //if internal node
	NodeKey    *nodeKey  //if leaf node
	Count      int
	//
	appliedOperations []Operation
}

type nodeLink struct {
	NextNodeKey *nodeKey
	LinkValue   *node
}

type nodeKey struct {
	Key          int
	NextNodeLink *nodeLink //if internal node
	NextKey      *nodeKey  //if leaf key
	Value        string
}

//end internal node region

//operations region
type Operation struct {
	Type           int
	Key            int
	Value          string
	InfluencedNode *node
}

//end operation region
