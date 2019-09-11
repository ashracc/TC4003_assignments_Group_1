
package cos418_hw1_1

import (
	"bufio"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// TODO: implement me
	// HINT: use for loop over `nums`
	var tot int
	for i := range nums {
		tot += i
	}
	out <- tot
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers
	data, err := ioutil.ReadFile(fileName)
	checkError(err)
	numbers, err := readInts(strings.NewReader(string(data)))
	checkError(err)

	inChan  := make(chan int, len(numbers)/num)
	outChan := make(chan int, num)
	
	for x := 0; x < num; x++ {
		go sumWorker(inChan, outChan)
	}
	for n := range numbers {
		inChan <- numbers[n]
	}
	close(inChan)

	var result int
	for x := 0; x < num; x++ {
		result += <- outChan
	}
	close(outChan)
	
	return result
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}