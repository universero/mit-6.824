package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

var unlock = "unlock"

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	id string // 持有者ID
	l  string // 锁的资源
}

// MakeLock The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, id: kvtest.RandValue(8), l: l}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		// 自旋获取key
		value, version, err := lk.ck.Get(lk.l)
		if value == lk.id { // 已持有锁
			return
		} else if err == rpc.ErrNoKey || value == unlock { // 未加锁
			// 尝试加锁
			if err = lk.ck.Put(lk.l, lk.id, version); err == rpc.OK {
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	for {
		// 自旋获取key
		value, version, err := lk.ck.Get(lk.l)
		if value != lk.id || err == rpc.ErrNoKey { // 未持有锁
			return
		} else { // 尝试释放锁
			if err = lk.ck.Put(lk.l, unlock, version); err == rpc.OK {
				return
			}
		}
	}
}
