package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
)

var waitGroup sync.WaitGroup
var waitCreateResultGroup sync.WaitGroup
var ch = make(chan Row, 1000)

func main() {
	waitCreateResultGroup.Add(1)
	go createResultCsv()
	for i := 0; i < 10; i++ {
		waitGroup.Add(1)
		go loadingDataFromCsv()
	}
	waitGroup.Wait()
	close(ch)
	waitCreateResultGroup.Wait()
}

func createResultCsv() {
	count := 0
	linkedLists := make(map[int64]*LinkedList)
	for r := range ch {
		if count < 1000 {
			if list, ok := linkedLists[r.ID]; ok {
				if list.Count < 20 {
					list.putNode(r)
					count = count + 1
				} else {
					if list.Head.Value.Price > r.Price {
						list.removeHead()
						list.putNode(r)
						count = count + 1
					}
				}
			} else {
				newList := LinkedList{Key: r.ID, Count: 0}
				newList.putNode(r)
				linkedLists[r.ID] = &newList
				count = count + 1
			}
		} else {
			if list, ok := linkedLists[r.ID]; ok {
				if list.Count < 20 {
					maxRow := Row{
						Price: math.MinInt16,
					}
					var maxList *LinkedList
					for _, llist := range linkedLists {
						if llist.Head != nil && llist.Head.Value.Price > maxRow.Price {
							maxRow = llist.Head.Value
							maxList = llist
						}
					}
					if maxList != nil && maxList.Head.Value.Price > r.Price {
						list.putNode(r)
						maxList.removeHead()
						if maxList.Count == 0 {
							delete(linkedLists, maxList.Key)
						}
					}
				} else {
					if list.Head.Value.Price > r.Price {
						list.removeHead()
						list.putNode(r)
					}
				}
			} else {
				maxRow := Row{
					Price: math.MinInt16,
				}
				var maxList *LinkedList
				for _, llist := range linkedLists {
					if llist.Head != nil && llist.Head.Value.Price > maxRow.Price {
						maxRow = llist.Head.Value
						maxList = llist
					}
				}
				if maxList != nil && maxList.Head.Value.Price > r.Price {
					newList := LinkedList{Key: r.ID, Count: 0}
					newList.putNode(r)
					linkedLists[r.ID] = &newList
					maxList.removeHead()
					if maxList.Count == 0 {
						delete(linkedLists, maxList.Key)
					}
				}
			}
		}
	}
	for key, list := range linkedLists {
		log.Println(key)
		list.PrintAll()
	}
	waitCreateResultGroup.Done()
}

func loadingDataFromCsv() {
	for i := 0; i < 1000; i++ {
		row := Row{
			ID:    rand.Int63n(10),
			Price: rand.Int63n(1000),
			Name:  "hello",
		}
		ch <- row
	}
	waitGroup.Done()
}

//Row struct
type Row struct {
	ID    int64
	Price int64
	Name  string
}

//LinkedList struct
type LinkedList struct {
	Key   int64
	Count int
	Head  *Node
}

//PrintAll func
func (ll *LinkedList) PrintAll() {
	output := ""
	cursor := ll.Head
	for cursor != nil {
		output += fmt.Sprintf("%#v", cursor.Value)
		output += "\n"
		cursor = cursor.Next
	}
	log.Println(output)
}

func (ll *LinkedList) putNode(row Row) {
	newNode := Node{
		Value: row,
		Next:  nil,
	}
	cursor := ll.Head
	var previous *Node
	for cursor != nil && cursor.Value.Price >= newNode.Value.Price {
		previous = cursor
		cursor = cursor.Next
	}
	newNode.Next = cursor
	if previous != nil {
		previous.Next = &newNode
	} else {
		ll.Head = &newNode
	}
	ll.Count = ll.Count + 1
}

func (ll *LinkedList) removeHead() {
	if ll.Head != nil {
		ll.Head = ll.Head.Next
	}
	ll.Count = ll.Count - 1
}

//Node struct
type Node struct {
	Value Row
	Next  *Node
}
