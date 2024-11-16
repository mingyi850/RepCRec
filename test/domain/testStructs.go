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

type TransactionManagerTestImpl struct {
	transactionManager *domain.TransactionManagerImpl
}

func CreateTransactionManagerTestImpl(siteCoordinator domain.SiteCoordinator) *TransactionManagerTestImpl {
	return &TransactionManagerTestImpl{
		transactionManager: domain.CreateTransactionManager(siteCoordinator),
	}
}

func (t *TransactionManagerTestImpl) Begin(transaction int, time int) error {
	return t.transactionManager.Begin(transaction, time)
}

func (t *TransactionManagerTestImpl) End(transaction int, time int) (domain.CommitResult, error) {
	return t.transactionManager.End(transaction, time)
}

func (t *TransactionManagerTestImpl) Write(transaction int, key int, value int, time int) (domain.WriteResult, error) {
	return t.transactionManager.Write(transaction, key, value, time)
}

func (t *TransactionManagerTestImpl) Read(transaction int, key int, time int) (domain.ReadResult, error) {
	return t.transactionManager.Read(transaction, key, time)
}

func (t *TransactionManagerTestImpl) Recover(site int, time int) error {
	return t.transactionManager.Recover(site, time)
}

/****************************************************
 * Helper functions for testing
 ****************************************************/

/*
*************
SiteCoordinator
*************
*/
func (s *SiteCoordinatorTestImpl) GetLatestValue(site int, key int) domain.HistoricalValue {
	return s.siteCoordinator.Sites[site].GetLastCommitted(key)
}

/*******************
 TransactionManager
*******************/
