// Package main implements the tool.
package main

import (
	"fmt"

	"github.com/udhos/consistenthash/consistenthash"
)

func main() {
	h := consistenthash.New(consistenthash.Options{})

	hosts := []string{"host1", "host2", "host3"}

	h.AddNodes(hosts)

	keys := []string{"info1", "info2", "info3", "info4", "info5", "info6"}

	for _, k := range keys {
		host := h.GetNode(k)
		fmt.Printf("%s: %s\n", k, host)
	}

	for _, k := range keys {
		host := h.GetNode(k)
		fmt.Printf("%s: %s\n", k, host)
	}
}
