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
	Index int
	URL   string
	Body  []byte
	Err   error
	Resp  *http.Response
}

func curl(ctx context.Context, client *http.Client, index int, url string) Result {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Result{Index: index, URL: url, Err: err}
	}

	resp, err := client.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return Result{Index: index, URL: url, Err: err}
	}

	defer resp.Body.Close()

	// Сначала читаем тело целиком, чтобы не выводить
	// неполный ответ, если чтение завершится ошибкой.
	body, err := io.ReadAll(resp.Body)
	if err != nil {

		return Result{Index: index, URL: url, Err: err}
	}

	return Result{
		Index: index,
		URL:   url,
		Resp:  resp,
		Body:  body,
	}
}

func isTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func printResponse(resp *http.Response, body []byte) error {

	fmt.Printf("%s %s\n", resp.Proto, resp.Status)

	keys := make([]string, 0, len(resp.Header))
	for key := range resp.Header {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		for _, value := range resp.Header[key] {
			fmt.Printf("%s: %s\n", key, value)
		}
	}

	fmt.Println()
	_, err := os.Stdout.Write(body)
	if err != nil {
		return fmt.Errorf("ошибка вывода тела ответа: %w", err)
	}

	return nil
}

func run() int {
	var timeoutSeconds int
	var showHelp bool

	fs := flag.NewFlagSet("hedgedcurl", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.IntVar(&timeoutSeconds, "t", 15, "таймаут запросов в секундах")
	fs.IntVar(&timeoutSeconds, "timeout", 15, "таймаут запросов в секундах")
	fs.BoolVar(&showHelp, "h", false, "показать справку")
	fs.BoolVar(&showHelp, "help", false, "показать справку")

	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Использование: %s [флаги] URL...\n\n", os.Args[0])
		fmt.Fprintln(fs.Output(), "Выполняет параллельные HTTP GET-запросы и выводит первый успешный HTTP-ответ.")
		fmt.Fprintln(fs.Output(), "\nФлаги:")
		fs.PrintDefaults()
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 1
	}

	if showHelp {
		fs.Usage()
		return 0
	}

	if timeoutSeconds <= 0 {
		fmt.Fprintln(os.Stderr, "Ошибка: таймаут должен быть больше 0")
		return 1
	}

	urls := fs.Args()
	if len(urls) == 0 {
		fmt.Fprintln(os.Stderr, "Ошибка: укажите хотя бы один URL")
		fs.Usage()
		return 1
	}

	timeout := time.Duration(timeoutSeconds) * time.Second
	client := &http.Client{}

	results := make(chan Result, len(urls))
	cancels := make([]context.CancelFunc, len(urls))

	// Запускаем все запросы параллельно.
	for i, url := range urls {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		cancels[i] = cancel

		go func(index int, url string, ctx context.Context) {
			results <- curl(ctx, client, index, url)
		}(i, url, ctx)
	}

	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	timeoutOccurred := false

	// Продолжаем, пока не получим HTTP-ответ
	// или пока не завершатся все запросы с ошибками.
	for range urls {
		result := <-results

		if result.Err != nil {
			fmt.Fprintf(
				os.Stderr,
				"Ошибка запроса %s: %v\n",
				result.URL,
				result.Err,
			)

			if isTimeout(result.Err) {
				timeoutOccurred = true
			}
			continue
		}

		// Получили HTTP-ответ. Отменяем остальные запросы,
		// но оставляем выбранный запрос активным до чтения тела.
		for i, cancel := range cancels {
			if i != result.Index {
				cancel()
			}
		}

		err := printResponse(result.Resp, result.Body)
		cancels[result.Index]()

		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			if isTimeout(err) {
				return timeoutExitCode
			}
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
