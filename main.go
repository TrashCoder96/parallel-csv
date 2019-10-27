package main

import (
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
	linkedLists := make(map[int64]LinkedList)
	for r := range ch {
		if count < 1000 {
			if list, ok := linkedLists[r.ID]; ok {
				if list.Count < 20 {
					list.putNode(r)
					count++
				} else {
					if list.Head.Value.Price > r.Price {
						list.removeHead()
						list.putNode(r)
						count++
					}
				}
			} else {
				newList := LinkedList{}
				newList.putNode(r)
				linkedLists[r.ID] = newList
				count++
			}
		} else {
			if list, ok := linkedLists[r.ID]; ok {
				if list.Count < 20 {
					//find entry with max price, remove this entry and put new entry to map[r.id], if map[r.ID].count < 20
					maxRow := Row{
						Price: math.MinInt16,
					}
					var maxList *LinkedList
					for _, llist := range linkedLists {
						if llist.Head != nil && llist.Head.Value.Price > maxRow.Price {
							maxRow = llist.Head.Value
							maxList = &llist
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
				//find entry with max price, remove from this entry node and put new node to map[r.id] entry
				maxRow := Row{
					Price: math.MinInt16,
				}
				var maxList *LinkedList
				for _, llist := range linkedLists {
					if llist.Head != nil && llist.Head.Value.Price > maxRow.Price {
						maxRow = llist.Head.Value
						maxList = &llist
					}
				}
				if maxList != nil && maxList.Head.Value.Price > r.Price {
					newList := LinkedList{}
					newList.putNode(r)
					linkedLists[r.ID] = newList
					maxList.removeHead()
					if maxList.Count == 0 {
						delete(linkedLists, maxList.Key)
					}
				}
			}
		}
	}
	waitCreateResultGroup.Done()
}

func loadingDataFromCsv() {
	for i := 0; i < 10; i++ {
		row := Row{
			ID:    int64(i),
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

func (ll *LinkedList) putNode(row Row) {
	newNode := Node{
		Value:    row,
		Next:     nil,
		Previous: nil,
	}
	cursor := ll.Head
	for cursor != nil && cursor.Value.Price >= newNode.Value.Price {
		cursor = cursor.Next
	}
	if cursor != nil {
		newNode.Next = cursor
		newNode.Previous = cursor.Previous
		if cursor.Previous != nil {
			cursor.Previous.Next = &newNode
			cursor.Previous = &newNode
		}
	} else {
		ll.Head = &newNode
	}
	ll.Count++
}

func (ll LinkedList) removeHead() {
	if ll.Head != nil {
		ll.Head = ll.Head.Next
	}
	ll.Count--
}

//Node struct
type Node struct {
	Value    Row
	Next     *Node
	Previous *Node
}
