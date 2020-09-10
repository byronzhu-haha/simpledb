package skiplist

type node struct {
	forward    []*node
	backward   *node
	key, value interface{}
}

func (n *node) next() *node {
	if len(n.forward) == 0 {
		return nil
	}
	return n.forward[0]
}

func (n *node) hasNext() bool {
	return n.next() != nil
}
