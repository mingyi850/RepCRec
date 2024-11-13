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

type TransasctionManager = domain.TransactionManagerImpl
type SiteCoordinator = domain.SiteCoordinatorImpl

func Simulation(file *os.File) error {
	siteCoordidnator := domain.CreateSiteCoordinator(10)
	transactionManager := domain.CreateTransactionManager(siteCoordidnator)

	time := 1
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println(line)
		switch {
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
			fmt.Printf("T%d %s\n", transaction, result.String())
		case isWrite(line):
			transaction, key, value, err := extractWrite(line)
			if err != nil {
				return err
			}
			err = transactionManager.Write(transaction, key, value, time)
			if err != nil {
				return err
			}
		case isRead(line):
			transaction, key, err := extractRead(line)
			if err != nil {
				return err
			}
			value, err := transactionManager.Read(transaction, key, time)
			if err != nil {
				return err
			}
			switch value.ResultType {
			case domain.Success:
				fmt.Println(value.Value)
			case domain.Abort:
				fmt.Printf("T%d aborts\n", transaction)
			case domain.Wait:
				fmt.Printf("T%d waits\n", transaction)
			}
		case isFail(line):
			site, err := extractFail(line)
			if err != nil {
				return err
			}
			siteCoordidnator.Fail(site, time)
		case isRecover(line):
			site, err := extractRecover(line)
			if err != nil {
				return err
			}
			siteCoordidnator.Recover(site, time)
			transactionManager.Recover(site, time)
		case isDump(line):
			result := siteCoordidnator.Dump()
			fmt.Println(result)
		default:
			return fmt.Errorf("could not parse line %q", line)
		}
		/*
			if isBegin(line) {
				transaction, err := extractBegin(line)
				if err != nil {
					return err
				}
				if err = transactionManager.Begin(transaction, time); err != nil {
					fmt.Println(err)
					return err
				}
			} else if isEnd(line) {
				transaction, err := extractEnd(line)
				if err != nil {
					return err
				}
				result, err := transactionManager.End(transaction, time)
				if err != nil {
					return err
				}
				fmt.Printf("T%d %s\n", transaction, result.String())
			} else if isWrite(line) {
				transaction, key, value, err := extractWrite(line)
				if err != nil {
					return err
				}
				err = transactionManager.Write(transaction, key, value, time)
				if err != nil {
					return err
				}
			} else if isRead(line) {
				transaction, key, err := extractRead(line)
				if err != nil {
					return err
				}
				value, err := transactionManager.Read(transaction, key, time)
				if err != nil {
					return err
				}
				if value != -1 {
					fmt.Println(value)
				}
			} else if isFail(line) {
				site, err := extractFail(line)
				if err != nil {
					return err
				}
				siteCoordidnator.Fail(site, time)
			} else if isRecover(line) {
				site, err := extractRecover(line)
				if err != nil {
					return err
				}
				siteCoordidnator.Recover(site, time)
				transactionManager.Recover(site, time)
			} else if isDump(line) {
				result := siteCoordidnator.Dump()
				fmt.Println(result)
			} else {
				return fmt.Errorf("could not parse line %q", line)
			}*/
		time++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
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
