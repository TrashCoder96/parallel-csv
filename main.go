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
	for i := 2; i < 10000; i++ {
		l.insertNode(rand.Intn(3000), "hello")
	}
	l.waitGroup.Done()
}

//LinkedList type
type LinkedList struct {
	waitGroup sync.WaitGroup
	Root      unsafe.Pointer
}

func (l *LinkedList) printAll() {
	var currentPointer = l.Root
	var previousPointer unsafe.Pointer
	for currentPointer != nil {
		node := (*Node)(currentPointer)
		previousPointer = currentPointer
		currentPointer = node.Next
		log.Println(node.Key)
		if currentPointer != nil && previousPointer != nil && (*Node)(previousPointer).Key > (*Node)(currentPointer).Key {
			log.Println("ERROR")
		}
	}
}

func initList(key int, value string) *LinkedList {
	list := &LinkedList{}
	list.Root = unsafe.Pointer(&Node{
		Key:   key,
		Value: value,
	})
	return list
}

func (l *LinkedList) insertNode(key int, value string) {
	newNode := Node{
		Key:   key,
		Value: value,
	}
	var currentPointer = (*Node)(l.Root)
	var previousPointer *Node
	for currentPointer != nil && currentPointer.Key <= key {
		previousPointer = currentPointer
		currentPointer = (*Node)(currentPointer.Next)
	}
	l.insertBeetween(previousPointer, &newNode, currentPointer)
}

func (l *LinkedList) insertBeetween(previuos *Node, new *Node, next *Node) bool {
	unsafeNew := unsafe.Pointer(new)
	unsafeNext := unsafe.Pointer(next)
	a := true
	if previuos != nil {
		a = a && atomic.CompareAndSwapPointer(&previuos.Next, unsafeNext, unsafeNew)
	} else {
		a = a && atomic.CompareAndSwapPointer(&l.Root, unsafeNext, unsafeNew)
	}
	a = a && atomic.CompareAndSwapPointer(&new.Next, nil, unsafeNext)
	return a
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
