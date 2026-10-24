package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"time"
)

const timeoutExitCode = 228

type Result struct {
	URL  string
	Resp *http.Response
	Err  error
}

func curl2(ctx context.Context, client *http.Client, url string) Result {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Result{URL: url, Err: err}
	}

	resp, err := client.Do(req)
	return Result{URL: url, Resp: resp, Err: err}
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func printResponse(resp *http.Response) error {
	defer resp.Body.Close()

	fmt.Printf("%s %s\n", resp.Proto, resp.Status)

	keys := make([]string, 0, len(resp.Header))
	for k := range resp.Header {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		for _, v := range resp.Header[k] {
			fmt.Printf("%s: %s\n", k, v)
		}
	}

	fmt.Println()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("ошибка чтения тела ответа: %w", err)
	}

	fmt.Print(string(body))
	return nil
}

func run() int {
	var timeoutSeconds int

	flag.IntVar(&timeoutSeconds, "t", 15, "таймаут запросов в секундах")
	flag.IntVar(&timeoutSeconds, "timeout", 15, "таймаут запросов в секундах")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Использование: %s [флаги] URL...\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if timeoutSeconds <= 0 {
		fmt.Fprintln(os.Stderr, "Ошибка: таймаут должен быть больше 0")
		return 1
	}

	urls := flag.Args()
	if len(urls) == 0 {
		fmt.Fprintln(os.Stderr, "Ошибка: укажите хотя бы один URL")
		flag.Usage()
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &http.Client{
		Timeout: time.Duration(timeoutSeconds) * time.Second,
	}

	results := make(chan Result, len(urls))

	for _, url := range urls {
		go func(url string) {
			results <- curl2(ctx, client, url)
		}(url)
	}

	timeoutOccurred := false

	for range urls {
		result := <-results

		if result.Err != nil {
			fmt.Fprintf(os.Stderr, "Ошибка запроса %s: %v\n",
				result.URL, result.Err)

			if isTimeout(result.Err) {
				timeoutOccurred = true
			}
			continue
		}

		// Получен первый HTTP-ответ.
		// Отменяем остальные запросы.
		cancel()

		if err := printResponse(result.Resp); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}

		return 0
	}

	fmt.Fprintln(os.Stderr, "Ошибка: все запросы завершились неудачно")

	if timeoutOccurred {
		return timeoutExitCode
	}
	return 1
}

func main() {
	os.Exit(run())
}
