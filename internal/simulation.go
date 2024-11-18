package internal

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/mingyi850/repcrec/internal/domain"
)

func Simulation(file *os.File, siteCoordinator domain.SiteCoordinator, transactionManager domain.TransactionManager) error {
	time := 1
	scanner := bufio.NewScanner(file)
	commentFlag := false
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println(line)
		switch {
		case isCommentStart(line):
			commentFlag = true
			continue
		case isCommentEnd(line):
			commentFlag = false
			continue
		case isComment(line, commentFlag):
			continue
		case line == "":
			continue
		case isBegin(line):
			transaction, err := extractBegin(line)
			if err != nil {
				return err
			}
			if err = transactionManager.Begin(transaction, time); err != nil {
				fmt.Println(err)
				return err
			}
		case isEnd(line):
			transaction, err := extractEnd(line)
			if err != nil {
				return err
			}
			result, err := transactionManager.End(transaction, time)
			if err != nil {
				return err
			}
			domain.HandleCommitResult(transaction, result)
		case isWrite(line):
			transaction, key, value, err := extractWrite(line)
			if err != nil {
				return err
			}
			result, err := transactionManager.Write(transaction, key, value, time)
			if err != nil {
				return err
			}
			domain.HandleWriteResult(transaction, key, result)
		case isRead(line):
			transaction, key, err := extractRead(line)
			if err != nil {
				return err
			}
			value, err := transactionManager.Read(transaction, key, time)
			if err != nil {
				return err
			}
			domain.HandleReadResult(transaction, key, value)
		case isFail(line):
			site, err := extractFail(line)
			if err != nil {
				return err
			}
			siteCoordinator.Fail(site, time)
		case isRecover(line):
			site, err := extractRecover(line)
			if err != nil {
				return err
			}
			siteCoordinator.Recover(site, time)
			transactionManager.Recover(site, time)
		case isDump(line):
			result := siteCoordinator.Dump()
			fmt.Println(result)
		default:
			return fmt.Errorf("could not parse line %q", line)
		}
		time++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func isCommentStart(line string) bool {
	return strings.HasPrefix(line, "/*")
}

func isCommentEnd(line string) bool {
	return strings.HasSuffix(line, "*/")
}
func isComment(line string, commentFlag bool) bool {
	return commentFlag || strings.HasPrefix(line, "//")
}
func isBegin(line string) bool {
	return strings.HasPrefix(line, "begin")
}

func isEnd(line string) bool {
	return strings.HasPrefix(line, "end")
}

func isWrite(line string) bool {
	return strings.HasPrefix(line, "W(")
}

func isRead(line string) bool {
	return strings.HasPrefix(line, "R(")
}

func isFail(line string) bool {
	return strings.HasPrefix(line, "fail")
}

func isRecover(line string) bool {
	return strings.HasPrefix(line, "recover")
}

func isDump(line string) bool {
	return strings.HasPrefix(line, "dump")
}

// Example: R(T1, x4) -> 1, 4
func extractRead(line string) (int, int, error) {
	re := regexp.MustCompile(`R\(T(\d+),\s*x(\d+)\)`)
	matches := re.FindStringSubmatch(line)
	if len(matches) > 1 {
		tx, err := strconv.Atoi(matches[1])
		if err != nil {
			return -1, -1, fmt.Errorf("could not convert transaction ID in line %q: %v", line, err)
		}
		key, err := strconv.Atoi(matches[2])
		if err != nil {
			return -1, -1, fmt.Errorf("could not convert key ID in line %q: %v", line, err)
		}
		return tx, key, nil
	}
	return -1, -1, fmt.Errorf("could not extract read line %q", line)
}

// Example W(T2, x6, v) -> 2, 6, v
func extractWrite(line string) (int, int, int, error) {
	re := regexp.MustCompile(`W\(T(\d+),\s*x(\d+),\s*(\d+)\)`)
	matches := re.FindStringSubmatch(line)
	if len(matches) > 2 {
		tx, err := strconv.Atoi(matches[1])
		if err != nil {
			return -1, -1, -1, fmt.Errorf("could not convert transaction ID in line %q: %v", line, err)
		}
		key, err := strconv.Atoi(matches[2])
		if err != nil {
			return -1, -1, -1, fmt.Errorf("could not convert key ID in line %q: %v", line, err)
		}
		value, err := strconv.Atoi(matches[3])
		if err != nil {
			return -1, -1, -1, fmt.Errorf("could not convert value in line %q: %v", line, err)
		}
		return tx, key, value, nil
	}
	return -1, -1, -1, fmt.Errorf("could not extract write line %q", line)
}

// Example begin(T1) -> 1
func extractBegin(line string) (int, error) {
	re := regexp.MustCompile(`begin\(T(\d+)\)`)
	matches := re.FindStringSubmatch(line)
	if len(matches) > 1 {
		tx, err := strconv.Atoi(matches[1])
		if err != nil {
			return -1, fmt.Errorf("could not convert transaction ID in line %q: %v", line, err)
		}
		return tx, nil
	}
	return -1, fmt.Errorf("could not extract begin line %q", line)
}

// Example end(T1) -> 1
func extractEnd(line string) (int, error) {
	re := regexp.MustCompile(`end\(T(\d+)\)`)
	matches := re.FindStringSubmatch(line)
	if len(matches) > 1 {
		tx, err := strconv.Atoi(matches[1])
		if err != nil {
			return -1, fmt.Errorf("could not convert transaction ID in line %q: %v", line, err)
		}
		return tx, nil
	}
	return -1, fmt.Errorf("could not extract end line %q", line)
}

// Example fail(3) -> 3
func extractFail(line string) (int, error) {
	re := regexp.MustCompile(`fail\((\d+)\)`)
	matches := re.FindStringSubmatch(line)
	if len(matches) > 1 {
		site, err := strconv.Atoi(matches[1])
		if err != nil {
			return -1, fmt.Errorf("could not convert site ID in line %q: %v", line, err)
		}
		return site, nil
	}
	return -1, fmt.Errorf("could not extract fail line %q", line)
}

// Example recover(3) -> 3
func extractRecover(line string) (int, error) {
	re := regexp.MustCompile(`recover\((\d+)\)`)
	matches := re.FindStringSubmatch(line)
	if len(matches) > 1 {
		site, err := strconv.Atoi(matches[1])
		if err != nil {
			return -1, fmt.Errorf("could not convert site ID in line %q: %v", line, err)
		}
		return site, nil
	}
	return -1, fmt.Errorf("could not extract recover line %q", line)
}
