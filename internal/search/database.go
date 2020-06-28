package search

import (
	"fmt"
	"sync"

	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/gobwas/glob"
	"github.com/nick-jones/piscola/internal/gen-go/service"
)

type Database struct {
	sync.RWMutex
	ids  map[int32]*service.Item
	tree *redblacktree.Tree
}

func newDatabase() *Database {
	return &Database{
		ids:  make(map[int32]*service.Item),
		tree: redblacktree.NewWith(int32Comparator),
	}
}

func (d *Database) Add(item *service.Item) bool {
	d.Lock()
	defer d.Unlock()

	if _, found := d.ids[item.ID]; found {
		return false
	}

	d.ids[item.ID] = item
	d.addItemToTree(item)

	return true
}

func (d *Database) Lookup(query *service.Query) (*service.Result_, error) {
	pattern, err := glob.Compile(query.Pattern)
	if err != nil {
		return nil, err
	}

	res := &service.Result_{}

	d.RLock()
	defer d.RUnlock()

	it := d.tree.Iterator()
	it.End()
outer:
	for it.Prev() {
		items := it.Value().([]*service.Item)
		for _, item := range items {
			if item.Deleted || !pattern.Match(item.Value) {
				continue
			}

			if res.Total < query.Limit {
				res.Ids = append(res.Ids, item.ID)
			}
			res.Total++

			if res.Total == query.Limit && !query.TotalRequired {
				break outer
			}
		}
	}

	return res, nil
}

func (d *Database) LookupAdvanced(query *service.AdvancedQuery) (*service.Result_, error) {
	res := &service.Result_{}

	include := make([]glob.Glob, len(query.IncludePatterns))
	for i, pattern := range query.IncludePatterns {
		incl, err := glob.Compile(pattern)
		if err != nil {
			return nil, err
		}
		include[i] = incl
	}

	exclude := make([]glob.Glob, len(query.ExcludePatterns))
	for i, pattern := range query.ExcludePatterns {
		excl, err := glob.Compile(pattern)
		if err != nil {
			return nil, err
		}
		exclude[i] = excl
	}

	d.RLock()
	defer d.RUnlock()

	it := d.tree.Iterator()
	it.End()
outer:
	for it.Prev() {
		items := it.Value().([]*service.Item)
		for _, item := range items {
			if item.Deleted && !query.IncludeDeleted {
				continue
			}
			if !itemHasAttributes(item, query.WithAttributes) {
				continue
			}
			if !matchAll(item.Value, include, exclude) {
				continue
			}

			if res.Total < query.Limit {
				res.Ids = append(res.Ids, item.ID)
			}
			res.Total++

			if res.Total == query.Limit && !query.TotalRequired {
				break outer
			}
		}
	}

	return res, nil
}

func (d *Database) Put(item *service.Item) bool {
	d.Lock()
	defer d.Unlock()

	current, found := d.ids[item.ID]
	if found {
		if item.Time != current.Time {
			val, found := d.tree.Get(item.Time)
			if !found {
				panic(fmt.Sprintf("invalid state, %d missing from tree", item.ID))
			}
			d.tree.Put(item.Time, filter(val.([]*service.Item), item.ID))
			d.addItemToTree(item)
		}
		*current = *item
	} else {
		d.ids[item.ID] = item
		d.addItemToTree(item)
	}

	return true
}

func (d *Database) Remove(id int32) bool {
	d.Lock()
	defer d.Unlock()

	item, found := d.ids[id]
	if !found {
		return false
	}

	val, found := d.tree.Get(item.Time)
	if !found {
		panic(fmt.Sprintf("invalid state, %d missing from tree", item.ID))
	}

	d.tree.Put(item.Time, filter(val.([]*service.Item), id))
	delete(d.ids, id)

	return true
}

func (d *Database) Size() int {
	d.RLock()
	defer d.RUnlock()

	return len(d.ids)
}

func (d *Database) addItemToTree(item *service.Item) {
	var items []*service.Item
	val, found := d.tree.Get(item.Time)
	if found {
		items = val.([]*service.Item)
	}
	items = append(items, item)
	d.tree.Put(item.Time, items)
}

func int32Comparator(a, b interface{}) int {
	aAsserted := a.(int32)
	bAsserted := b.(int32)
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

func itemHasAttributes(item *service.Item, attrs service.Attrs) bool {
	for attr, val := range attrs {
		if itemVal, found := item.Attributes[attr]; !found || val != itemVal {
			return false
		}
	}
	return true
}

func matchAll(str string, include, exclude []glob.Glob) bool {
	for _, pattern := range include {
		if !pattern.Match(str) {
			return false
		}
	}
	for _, pattern := range exclude {
		if pattern.Match(str) {
			return false
		}
	}
	return true
}

func filter(items []*service.Item, id int32) []*service.Item {
	n := 0
	for _, item := range items {
		if item.ID != id {
			items[n] = item
			n++
		}
	}
	return items[:n]
}