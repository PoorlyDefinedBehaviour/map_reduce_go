package queue

import (
	"container/list"
)

type Queue[T any] struct {
	list *list.List
}

func New[T any]() *Queue[T] {
	return &Queue[T]{
		list: list.New(),
	}
}

func (queue *Queue[T]) Add(element T) {
	queue.list.PushFront(element)
}

func (queue *Queue[T]) Pop() (T, bool) {
	element := queue.list.Back()
	if element == nil {
		var v T
		return v, false
	}

	return queue.list.Remove(element).(T), true
}

func (queue *Queue[T]) Len() int {
	return queue.list.Len()
}
