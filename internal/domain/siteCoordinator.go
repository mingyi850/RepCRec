package domain

import (
	"fmt"
	"strings"

	"github.com/mingyi850/repcrec/internal/utils"
)

type SiteCommitResult string

const (
	SiteOk    SiteCommitResult = "success"
	SiteDown  SiteCommitResult = "down"
	SiteStale SiteCommitResult = "stale"
)

type Range struct {
	start int
	end   int
}

type SiteCoordinator interface {
	Fail(site int, time int) error
	Recover(site int, time int) error
	Dump() string
	ReadActiveSite(site int, key int, time int) (HistoricalValue, error)
	GetSitesForKey(key int) []int
	GetActiveSitesForKey(key int) []int
	GetValidSitesForRead(key int, txStart int) []int
	VerifySiteWrite(site int, key int, writeTime int, currentTime int) SiteCommitResult
	CommitSiteWrite(site int, key int, value int, time int) error
}

type SiteCoordinatorImpl struct {
	Sites      map[int]DataManager
	SiteUptime map[int]([]Range)
}

func CreateSiteCoordinator(numSites int) *SiteCoordinatorImpl {
	sites := make(map[int]DataManager)
	uptimes := make(map[int]([]Range))
	for i := 1; i <= numSites; i++ {
		site := CreateDataManager(i)
		sites[i] = &site
		uptimes[i] = append(uptimes[i], Range{start: -1, end: -1})
	}
	return &SiteCoordinatorImpl{
		Sites:      sites,
		SiteUptime: uptimes,
	}
}

func (s *SiteCoordinatorImpl) Fail(site int, time int) error {
	if s.isActiveSite(site) {
		uptimeArr := s.SiteUptime[site]
		uptimeArr[len(uptimeArr)-1].end = time
	}
	return nil
}

func (s *SiteCoordinatorImpl) Recover(site int, time int) error {
	if !s.isActiveSite(site) {
		s.SiteUptime[site] = append(s.SiteUptime[site], Range{start: time, end: -1})
	}
	return nil
}

func (s *SiteCoordinatorImpl) Dump() string {
	results := make([]string, 10)
	for i := 1; i <= 10; i++ {
		results[i-1] = s.Sites[i].Dump()
	}
	return strings.Join(results, "\n")
}

func (s *SiteCoordinatorImpl) GetActiveSitesForKey(key int) []int {
	readSites := s.GetSitesForKey(key)
	result := make([]int, 0)
	for _, site := range readSites {
		if s.isActiveSite(site) {
			result = append(result, site)
		}
	}
	return result
}

func (s *SiteCoordinatorImpl) GetValidSitesForRead(key int, txStart int) []int {
	readSites := s.GetSitesForKey(key)
	result := make([]int, 0)
	if len(readSites) == 1 { // Odd case -> Return the non replicated site
		result = append(result, readSites[0])
	} else {
		for _, site := range readSites { // Even case -> Return sites which were alive between prev commit and Tx start
			historicRead := s.Sites[site].Read(key, txStart)
			if s.wasAliveBetween(site, historicRead.time, txStart) {
				result = append(result, site)
			}
		}
	}
	return result
}

func (s *SiteCoordinatorImpl) GetSitesForKey(key int) []int {
	if key%2 == 0 {
		return utils.GetRange(1, 10, 1)
	} else {
		return []int{1 + (key % 10)}
	}
}

func (s *SiteCoordinatorImpl) ReadActiveSite(site int, key int, time int) (HistoricalValue, error) {
	if !s.isActiveSite(site) {
		return HistoricalValue{}, fmt.Errorf("site %d is not active", site)
	}
	return s.Sites[site].Read(key, time), nil
}

func (s *SiteCoordinatorImpl) VerifySiteWrite(site int, key int, writeTime int, currentTime int) SiteCommitResult {
	if !s.wasAliveBetween(site, writeTime, currentTime) {
		return SiteDown
	}
	committedValue := s.Sites[site].GetLastCommitted(key)
	if committedValue.time < writeTime {
		return SiteOk
	} else {
		return SiteStale
	}
}

func (s *SiteCoordinatorImpl) CommitSiteWrite(site int, key int, value int, currentTime int) error {
	dataManager := s.Sites[site]
	dataManager.Commit(key, value, currentTime)
	return nil
}

func (s *SiteCoordinatorImpl) isActiveSite(site int) bool {
	uptimeArr := s.SiteUptime[site]
	return uptimeArr[len(uptimeArr)-1].end == -1
}

func (s *SiteCoordinatorImpl) wasAliveBetween(site int, start int, end int) bool {
	uptimeArr := s.SiteUptime[site]
	for _, uptime := range uptimeArr {
		if (uptime.start <= start) && (uptime.end >= end || uptime.end == -1) {
			return true
		}
	}
	return false
}
