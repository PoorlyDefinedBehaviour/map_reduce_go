package set

type Set[T comparable] struct {
	items map[T]struct{}
}

func New[T comparable]() *Set[T] {
	return &Set[T]{
		items: map[T]struct{}{},
	}
}

func (set *Set[T]) Add(item T) bool {
	_, itemWasInTheSet := set.items[item]

	set.items[item] = struct{}{}

	return itemWasInTheSet
}

func (set *Set[T]) Remove(item T) bool {
	_, itemWasInTheSet := set.items[item]

	delete(set.items, item)

	return itemWasInTheSet
}

func (set *Set[T]) Find(predicate func(*T) bool) (*T, bool) {
	for item := range set.items {
		if predicate(&item) {
			return &item, true
		}
	}

	return nil, false
}
