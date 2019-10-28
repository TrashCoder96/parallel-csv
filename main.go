package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
)

var waitGroup sync.WaitGroup
var waitCreateResultGroup sync.WaitGroup
var ch = make(chan Row, 1000)

func main() {
	waitCreateResultGroup.Add(1)
	go createResultCsv()
	for i := 1; i < len(os.Args); i++ {
		waitGroup.Add(1)
		go loadingDataFromCsv(os.Args[i])
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
	result := make([]Row, 0, 1000)
	for _, list := range linkedLists {
		cursor := list.Head
		for cursor != nil {
			result = append(result, cursor.Value)
			cursor = cursor.Next
		}
	}
	sort.Sort(RowSlice(result))
	log.Println(result)
	log.Println(len(result))
	waitCreateResultGroup.Done()
}

//RowSlice type
type RowSlice []Row

func (rs RowSlice) Len() int {
	return len(rs)
}

func (rs RowSlice) Less(i, j int) bool {
	return rs[i].Price < rs[j].Price
}

func (rs RowSlice) Swap(i, j int) {
	rs[i], rs[j] = rs[j], rs[i]
}

func loadingDataFromCsv(path string) {
	file, _ := os.Open(path)
	defer file.Close()
	reader := csv.NewReader(bufio.NewReader(file))
	reader.Comma = ';'
	line, err := reader.Read()
	for err != io.EOF {
		ID, _ := strconv.ParseInt(line[0], 10, 64)
		Name := line[1]
		Condition := line[2]
		State := line[3]
		Price, _ := strconv.ParseInt(line[4], 10, 64)
		row := Row{
			ID:        ID,
			Name:      Name,
			Condition: Condition,
			State:     State,
			Price:     Price,
		}
		ch <- row
		line, err = reader.Read()
	}
	waitGroup.Done()
}

//Row struct
type Row struct {
	ID        int64
	Name      string
	Condition string
	Price     int64
	State     string
}

//LinkedList struct
type LinkedList struct {
	Key   int64
	Count int
	Head  *Node
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
