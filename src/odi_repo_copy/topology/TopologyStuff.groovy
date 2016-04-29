package odi_repo_copy.topology

import oracle.odi.core.OdiInstance;
import oracle.odi.core.config.MasterRepositoryDbInfo
import oracle.odi.core.config.OdiConfigurationException;
import oracle.odi.core.config.OdiInstanceConfig
import oracle.odi.core.config.PoolingAttributes
import oracle.odi.core.security.Authentication
import oracle.odi.core.security.AuthenticationException;
import odi_repo_copy.user.UserStuff;

class TopologyStuff {
	static def REPO_CONNECTION_INFO;

	static main(args) {
		
	}
	
	
	static def TopologyStuff setREPO_CONNECTION_INFO(def repo_CONNECTION_INFO) {
		REPO_CONNECTION_INFO = repo_CONNECTION_INFO;
		return this;
	}

	static def changeWorkRepoUser(String workRepoName, String newRepoUser, String newRepoUserPassword){
		def odiInstance = createOdiInstanceOnlyMaster(REPO_CONNECTION_INFO['url'], REPO_CONNECTION_INFO['driver'], REPO_CONNECTION_INFO['master_repo_user'], REPO_CONNECTION_INFO['master_repo_password'], REPO_CONNECTION_INFO['odi_user'], REPO_CONNECTION_INFO['odi_password'])
		
		odiInstance.close();
	}
	
	static def OdiInstance createOdiInstanceOnlyMaster(String pMasterReposUrl, String pMasterReposDriver,
			String pMasterReposUser, String pMasterReposPassword, String pOdiUsername,
			String pOdiPassword) throws OdiConfigurationException, AuthenticationException {

		MasterRepositoryDbInfo masterInfo = new MasterRepositoryDbInfo(pMasterReposUrl, pMasterReposDriver,
				pMasterReposUser, pMasterReposPassword.toCharArray(), new PoolingAttributes());

		OdiInstance inst = OdiInstance.createInstance(new OdiInstanceConfig(masterInfo));

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
