package odi_repo_copy.topology

import oracle.odi.core.OdiInstance;
import oracle.odi.core.config.MasterRepositoryDbInfo
import oracle.odi.core.config.OdiConfigurationException;
import oracle.odi.core.config.OdiInstanceConfig
import oracle.odi.core.config.PoolingAttributes
import oracle.odi.core.config.WorkRepositoryDbInfo
import oracle.odi.core.persistence.IOdiEntityManager
import oracle.odi.core.persistence.transaction.ITransactionDefinition
import oracle.odi.core.persistence.transaction.ITransactionManager
import oracle.odi.core.persistence.transaction.ITransactionStatus
import oracle.odi.core.persistence.transaction.support.DefaultTransactionDefinition
import oracle.odi.core.security.Authentication
import oracle.odi.core.security.AuthenticationException;
import oracle.odi.domain.project.OdiProject
import oracle.odi.domain.project.finder.IOdiProjectFinder

class TopologyWorks {
	static def DB_CONNECTION_INFO = [
		url : 		"jdbc:oracle:thin:@odirepdb.tm.turkcell.tgc:1521/ODIREP",
		driver : 	"oracle.jdbc.OracleDriver",
		db_user : 	"ODI_DBA",
		db_pass : 	"p_odi_dba",
	];

	static def ODI_CONNECTION_INFO = [
		url : 		"jdbc:oracle:thin:@odirepdb.tm.turkcell.tgc:1521/ODIREP",
		driver : 	"oracle.jdbc.OracleDriver",
		repo_user : "ODIM_DEV_11G_TEMP",
		repo_pass : "ODIM_DEV_11G_TEMP",
		//repo_user : "ODIM_DEV_11G",
		//repo_pass : "p_ODIM_DEV_11G",
		//work_repo : "ODI_DEV_MAIN",
		work_repo : "ODI_DEV_MSW",
		odi_user : 	"BASARG",
		odi_pass:	"basarg"
	];

	static main(args) {
		//duplicate_repository_with_postfix("ODIM_DEV_SOLDWH_11G", "_20150902")
		OdiInstance odiInstance = createOdiInstance(
				ODI_CONNECTION_INFO['url'],
				ODI_CONNECTION_INFO['driver'],
				ODI_CONNECTION_INFO['repo_user'],
				ODI_CONNECTION_INFO['repo_pass'],
				ODI_CONNECTION_INFO['work_repo'],
				ODI_CONNECTION_INFO['odi_user'],
				ODI_CONNECTION_INFO['odi_pass']);
		ITransactionDefinition txnDef = new DefaultTransactionDefinition();
		ITransactionManager tm = odiInstance.getTransactionManager();
		ITransactionStatus txnStatus = tm.getTransaction(txnDef);
		IOdiEntityManager mgr = odiInstance.getTransactionalEntityManager();
		OdiProject proj = ((IOdiProjectFinder) mgr.getFinder(OdiProject.class)).findByCode("DATAWAREHOUSE");
		println('Found OdiProject: ' + proj.getCode());

	}

	static def OdiInstance createOdiInstance(String pMasterReposUrl, String pMasterReposDriver,
			String pMasterReposUser, String pMasterReposPassword, String pWorkReposName, String pOdiUsername,
			String pOdiPassword) throws OdiConfigurationException, AuthenticationException {

		MasterRepositoryDbInfo masterInfo = new MasterRepositoryDbInfo(pMasterReposUrl, pMasterReposDriver,
				pMasterReposUser, pMasterReposPassword.toCharArray(), new PoolingAttributes());
		WorkRepositoryDbInfo workInfo = null;

		if (pWorkReposName != null)
			workInfo = new WorkRepositoryDbInfo(pWorkReposName, new PoolingAttributes());

		OdiInstance inst = OdiInstance.createInstance(new OdiInstanceConfig(masterInfo, workInfo));

		try {
			Authentication auth = inst.getSecurityManager().createAuthentication(pOdiUsername, pOdiPassword.toCharArray());
			inst.getSecurityManager().setCurrentThreadAuthentication(auth);
			return inst;
		}
		catch (RuntimeException e) {
			inst.close();
			throw e;
		}
	}
}