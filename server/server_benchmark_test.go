package server_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/RussellLuo/goku/server"
)

func BenchmarkServer_Insert(b *testing.B) {
	s := server.NewServer()
	ts := time.Now().UnixNano()

	for i := 0; i < b.N; i++ {
		str := strconv.Itoa(i)
		s.Insert(0, "key"+str, "member"+str, ts, 2*time.Second)
	}
}

func BenchmarkServer_Insert_Parallel(b *testing.B) {
	s := server.NewServer()
	ts := time.Now().UnixNano()

	var i int
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			str := strconv.Itoa(i)
			s.Insert(0, "key"+str, "member"+str, ts, 2*time.Second)
			i++
		}
	})
}

func BenchmarkServer_Delete(b *testing.B) {
	s := server.NewServer()
	ts := time.Now().UnixNano()

	for i := 0; i < 1000000; i++ {
		str := strconv.Itoa(i)
		s.Insert(0, "key"+str, "member"+str, ts, 2*time.Second)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		str := strconv.Itoa(i)
		s.Delete(0, "key"+str, "member"+str, ts)
	}
}

func BenchmarkServer_Delete_Parallel(b *testing.B) {
	s := server.NewServer()
	ts := time.Now().UnixNano()

	for i := 0; i < 1000000; i++ {
		str := strconv.Itoa(i)
		s.Insert(0, "key"+str, "member"+str, ts, 2*time.Second)
	}
	b.ResetTimer()

	var i int
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			str := strconv.Itoa(i)
			s.Delete(0, "key"+str, "member"+str, ts)
			i++
		}
	})
}

func BenchmarkServer_Select(b *testing.B) {
	s := server.NewServer()
	ts := time.Now().UnixNano()

	for i := 0; i < 1000000; i++ {
		str := strconv.Itoa(i)
		s.Insert(0, "key"+str, "member"+str, ts, 2*time.Second)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		str := strconv.Itoa(i)
		s.Select(0, "key"+str, ts)
	}
}

func BenchmarkServer_Select_Parallel(b *testing.B) {
	s := server.NewServer()
	ts := time.Now().UnixNano()

	for i := 0; i < 1000000; i++ {
		str := strconv.Itoa(i)
		s.Insert(0, "key"+str, "member"+str, ts, 2*time.Second)
	}
	b.ResetTimer()

	var i int
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			str := strconv.Itoa(i)
			s.Select(0, "key"+str, ts)
			i++
		}
	})
}
