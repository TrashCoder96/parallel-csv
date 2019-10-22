package main

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"unsafe"
)

func main() {
	list := initList(1, "hello")
	for i := 0; i < 20; i++ {
		list.waitGroup.Add(1)
		go list.put()
	}
	list.waitGroup.Wait()
	list.printAll()
}

func (l *LinkedList) put() {
	for i := 2; i < 1000; i++ {
		l.insertNode(rand.Intn(30000), "hello")
	}
	l.waitGroup.Done()
}

//LinkedList type
type LinkedList struct {
	waitGroup   sync.WaitGroup
	Root        unsafe.Pointer
	LastMaxElem unsafe.Pointer
	count       *int32
}

func (l *LinkedList) printAll() {
	var currentPointer = l.Root
	var previousPointer unsafe.Pointer
	for currentPointer != nil {
		node := (*Node)(currentPointer)
		previousPointer = currentPointer
		currentPointer = node.Next
		log.Println(node.Key)
		if currentPointer != nil && previousPointer != nil && (*Node)(previousPointer).Key < (*Node)(currentPointer).Key {
			log.Println("ERROR")
		}
	}
	log.Println(*l.count)
}

func initList(key int, value string) *LinkedList {
	var c int32
	list := &LinkedList{count: &c}
	return list
}

func (l *LinkedList) insertNode(key int, value string) {
	newNode := Node{
		Key:   key,
		Value: value,
	}
	for {
		var currentPointer = (*Node)(l.Root)
		var previousPointer *Node
		for currentPointer != nil && currentPointer.Key >= key {
			previousPointer = currentPointer
			currentPointer = (*Node)(currentPointer.Next)
		}
		if l.insertBeetween(previousPointer, &newNode, currentPointer) {
			atomic.AddInt32(l.count, 1)
			break
		}
	}
}

func (l *LinkedList) insertBeetween(previuos *Node, new *Node, next *Node) bool {
	unsafeNew := unsafe.Pointer(new)
	unsafeNext := unsafe.Pointer(next)
	atomic.CompareAndSwapPointer(&new.Next, nil, unsafeNext)
	if previuos != nil {
		return atomic.CompareAndSwapPointer(&previuos.Next, unsafeNext, unsafeNew)
	}
	return atomic.CompareAndSwapPointer(&l.Root, unsafeNext, unsafeNew)
}

//Node type
type Node struct {
	Key   int
	Value string
	Next  unsafe.Pointer
}

//Row type
type Row struct {
	ID    int64
	Price int
}
