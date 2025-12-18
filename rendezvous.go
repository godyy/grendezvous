package grendezvous

import "sort"

// Hasher 计算字节切片的hash值.
type Hasher func(b []byte) uint64

// Rendezvous 是一个基于 rendezvous hashing 算法的负载均衡器.
type Rendezvous struct {
	nodes     []string
	nodeIndex map[string]int
	nodeHash  []uint64
	hasher    Hasher
}

// New 创建一个新的 Rendezvous 负载均衡器.
func New(nodes []string, hasher Hasher) *Rendezvous {
	if hasher == nil {
		panic("hasher is nil")
	}

	r := &Rendezvous{
		nodes:     make([]string, 0, len(nodes)),
		nodeIndex: make(map[string]int, len(nodes)),
		nodeHash:  make([]uint64, 0, len(nodes)),
		hasher:    hasher,
	}
	for _, node := range nodes {
		r.Add(node)
	}
	return r
}

// NewEmpty 创建一个空的 Rendezvous 负载均衡器.
func NewEmpty(hasher Hasher) *Rendezvous {
	return New(nil, hasher)
}

// Add 添加一个节点到负载均衡器.
func (r *Rendezvous) Add(node string) {
	// 检查节点是否存在
	if _, ok := r.nodeIndex[node]; ok {
		return
	}

	r.nodeIndex[node] = len(r.nodes)
	r.nodes = append(r.nodes, node)
	r.nodeHash = append(r.nodeHash, r.hasher([]byte(node)))
}

// Remove 从负载均衡器中移除一个节点.
func (r *Rendezvous) Remove(node string) {
	// 检查节点是否存在
	index, ok := r.nodeIndex[node]
	if !ok {
		return
	}

	// 移除节点.
	l := len(r.nodes) - 1
	if index != l {
		r.nodes[index] = r.nodes[l]
		r.nodeHash[index] = r.nodeHash[l]
		r.nodeIndex[r.nodes[l]] = index
	}
	r.nodes = r.nodes[:l]
	r.nodeHash = r.nodeHash[:l]
	delete(r.nodeIndex, node)
}

// Lookup 根据 key 查找对应的节点.
func (r *Rendezvous) Lookup(key []byte) string {
	// 如果没有节点，返回空字符串
	if len(r.nodes) == 0 {
		return ""
	}

	// 计算 key 的 hash 值
	keyHash := r.hasher(key)

	// 查找 hash 值最大的节点
	maxIndex := 0
	maxHash := xorshiftMult64(keyHash ^ r.nodeHash[0])
	for i, hash := range r.nodeHash[1:] {
		hash = xorshiftMult64(keyHash ^ hash)
		if hash > maxHash {
			maxHash = hash
			maxIndex = i + 1
		}
	}

	return r.nodes[maxIndex]
}

// LookupN 根据 key 查找hash只最高的n个节点.
func (r *Rendezvous) LookupN(key []byte, n int) []string {
	if n <= 0 || len(r.nodes) == 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	keyHash := r.hasher(key)
	idxs := make([]int, len(r.nodes))
	for i := range idxs {
		idxs[i] = i
	}
	sort.Slice(idxs, func(i, j int) bool {
		wi := xorshiftMult64(keyHash ^ r.nodeHash[idxs[i]])
		wj := xorshiftMult64(keyHash ^ r.nodeHash[idxs[j]])
		return wi > wj
	})
	res := make([]string, n)
	for i := 0; i < n; i++ {
		res[i] = r.nodes[idxs[i]]
	}
	return res
}

func xorshiftMult64(x uint64) uint64 {
	x ^= x >> 12 // a
	x ^= x << 25 // b
	x ^= x >> 27 // c
	return x * 2685821657736338717
}
