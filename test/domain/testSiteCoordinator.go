package test

import "github.com/mingyi850/repcrec/internal/domain"

type SiteCoordinatorTestImpl struct {
	siteCoordinator *domain.SiteCoordinatorImpl
}

func CreateSiteCoordinatorTestImpl(numSites int) *SiteCoordinatorTestImpl {
	return &SiteCoordinatorTestImpl{
		siteCoordinator: domain.CreateSiteCoordinator(numSites),
	}
}

func (s *SiteCoordinatorTestImpl) Fail(site int, time int) error {
	return s.siteCoordinator.Fail(site, time)
}

func (s *SiteCoordinatorTestImpl) Recover(site int, time int) error {
	return s.siteCoordinator.Recover(site, time)
}

func (s *SiteCoordinatorTestImpl) Dump() string {
	return s.siteCoordinator.Dump()
}

func (s *SiteCoordinatorTestImpl) ReadActiveSite(site int, key int, time int) (domain.HistoricalValue, error) {
	return s.siteCoordinator.ReadActiveSite(site, key, time)
}

func (s *SiteCoordinatorTestImpl) GetSitesForKey(key int) []int {
	return s.siteCoordinator.GetSitesForKey(key)
}

func (s *SiteCoordinatorTestImpl) GetActiveSitesForKey(key int) []int {
	return s.siteCoordinator.GetActiveSitesForKey(key)
}

func (s *SiteCoordinatorTestImpl) GetValidSitesForRead(key int, txStart int) []int {
	return s.siteCoordinator.GetValidSitesForRead(key, txStart)
}

func (s *SiteCoordinatorTestImpl) VerifySiteWrite(site int, key int, writeTime int, currentTime int) domain.SiteCommitResult {
	return s.siteCoordinator.VerifySiteWrite(site, key, writeTime, currentTime)
}

func (s *SiteCoordinatorTestImpl) CommitSiteWrite(site int, key int, value int, time int) error {
	return s.siteCoordinator.CommitSiteWrite(site, key, value, time)
}

/****************************************************
 * Helper functions for testing
 ****************************************************/
func (s *SiteCoordinatorTestImpl) GetLatestValue(site int, key int) domain.HistoricalValue {
	return s.siteCoordinator.Sites[site].GetLastCommitted(key)
}
