package skiplist

type Iterator interface {
	HasNext() bool
	Key() interface{}
	Value() interface{}
	Close()
}

type iterator struct {
	key, value interface{}
	currNode   *node
}

func (i *iterator) HasNext() bool {
	if !i.currNode.hasNext() {
		return false
	}
	i.currNode = i.currNode.next()
	i.key = i.currNode.key
	i.value = i.currNode.value

	return true
}

func (i *iterator) Key() interface{} {
	return i.key
}

func (i *iterator) Value() interface{} {
	return i.value
}

func (i *iterator) Close() {
	i.key = nil
	i.value = nil
	i.currNode = nil
}
