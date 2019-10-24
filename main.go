package main

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"unsafe"
)

func main() {
	list := ThreadSafeList{}
	for i := 0; i < 70; i++ {
		list.waitGroup.Add(1)
		go list.put()
	}
	list.waitGroup.Wait()
	go list.delete()
	list.printAll()
}

//ThreadSafeList type
type ThreadSafeList struct {
	count     int32
	head      *listNode
	waitGroup sync.WaitGroup
}

func (t *ThreadSafeList) delete() {
	for i := 0; i < 100; i++ {
		t.DeleteHead()
	}
}

func (t *ThreadSafeList) put() {
	for i := 999; i >= 0; i-- {
		t.InsertNode(rand.Intn(2000), "hello")
		atomic.AddInt32(&t.count, 1)
		//t.DeleteHead()
	}
	t.waitGroup.Done()
}

type listNode struct {
	markableNext *markablePointer
	key          int
	value        string
}

type markablePointer struct {
	marked bool
	next   *listNode
}

func (t *ThreadSafeList) printAll() {
	cursor := t.head
	i := 0
	for cursor != nil {
		i++
		log.Println(cursor.key)
		cursor = cursor.markableNext.next
	}
	log.Println(i)
	log.Println(t.count)
}

//InsertNode func
func (t *ThreadSafeList) InsertNode(key int, value string) {
	defer atomic.AddInt32(&t.count, 1)
	currentHeadAddress := &t.head
	currentHead := t.head
	if currentHead == nil || key < currentHead.key {
		newNode := listNode{
			key:   key,
			value: value,
			markableNext: &markablePointer{
				next: currentHead,
			},
		}
		operationSucceeded := atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(currentHeadAddress)),
			unsafe.Pointer(currentHead),
			unsafe.Pointer(&newNode),
		)
		if !operationSucceeded {
			t.InsertNode(key, value)
			return
		}
		return
	}
	cursor := t.head
	for {
		if cursor.markableNext.next == nil || key < cursor.markableNext.next.key {
			currentNext := cursor.markableNext
			if currentNext.marked {
				continue
			}
			newNode := listNode{
				key:   key,
				value: value,
				markableNext: &markablePointer{
					next: currentNext.next,
				},
			}
			newNext := markablePointer{
				next: &newNode,
			}
			operationSucceeded := atomic.CompareAndSwapPointer(
				(*unsafe.Pointer)(unsafe.Pointer(&(cursor.markableNext))),
				unsafe.Pointer(currentNext),
				unsafe.Pointer(&newNext),
			)
			if !operationSucceeded {
				t.InsertNode(key, value)
				return
			}
			break
		}
		cursor = cursor.markableNext.next
	}
}

//DeleteHead func
func (t *ThreadSafeList) DeleteHead() {
	defer atomic.AddInt32(&t.count, -1)
	currentHeadAddress := &t.head
	currentHead := t.head
	cursor := currentHead
	for {
		if cursor == nil {
			break
		}
		nextNode := cursor.markableNext.next
		newNext := markablePointer{
			marked: true,
			next:   nextNode,
		}
		operationSucceeded := atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(&(cursor.markableNext))),
			unsafe.Pointer(cursor.markableNext),
			unsafe.Pointer(&newNext),
		)
		if !operationSucceeded {
			t.DeleteHead()
			return
		}
		newNext = markablePointer{
			next: nextNode,
		}
		operationSucceeded = atomic.CompareAndSwapPointer(
			(*unsafe.Pointer)(unsafe.Pointer(currentHeadAddress)),
			unsafe.Pointer(currentHead),
			unsafe.Pointer(nextNode),
		)
		if !operationSucceeded {
			t.DeleteHead()
		}
		break
	}
}
