package odi_repo_copy.main
import java.lang.Class
import java.sql.DriverManager
import odi_repo_copy.user.UserStuff
import odi_repo_copy.topology.TopologyStuff

/*
 * TO-DO
 * Repo'da bi tablo tutsun. Hangi repo user'in ne amacla yaratildigi yazsin
 * TUI bana secenekleri sunsun; db'ye baglanip option'lari listelesin.
 */

class ODI_WORKS {
	static String PROPERTIES_FILE_PATH = "D:/DEV/Program Data/workspace-ggts-3.6.4.RELEASE/repo.properties";
	
	static def DB_CONNECTION_INFO = [:];

	static def REPO_CONNECTION_INFO = [:];

	static def mem = []

	static def readPropertiesFile(String pathToPropertiesFile){
		Properties prop = new Properties()
		File propertiesFile = new File(pathToPropertiesFile)
		propertiesFile.withInputStream {
			prop.load(it)
		}
		return prop;
	}
	
	static def loadDBConnection(def prop){
		prop.keys().each {
			if(it.indexOf("DB_CONNECTION_INFO") == 0){
				DB_CONNECTION_INFO[it.replace("DB_CONNECTION_INFO.", "")] = prop[it];	
			}
		}
	}
	static def loadRepoConnection(def prop){
		prop.keys().each {
			if(it.indexOf("REPO_CONNECTION_INFO") == 0){
				REPO_CONNECTION_INFO[it.replace("REPO_CONNECTION_INFO.", "")] = prop[it];
			}
		}
	}
	
	static initialize(){
		def prop = readPropertiesFile(PROPERTIES_FILE_PATH);
		loadDBConnection(prop)
		loadRepoConnection(prop)
	}
	
	static main(args) {
		initialize()
		String master_repo_user = "ODIM_DEV_11G";
		duplicate_repository_with_postfix(master_repo_user, "_20160426");
		
	}

	static def TUI(){
		def semaphore = 0
		printHelp()
		while(semaphore == 0){
			def raw_input = getInput("REPO_WORKS >> ")

			if(raw_input.equalsIgnoreCase("d")){
				println("## This will duplicate a repository recursively (copy work repo users with postfix as well).")
				println("## If source is ODIM, and target is ODIM_TEMP, then set postfix to '_TEMP'.")
				println("->Please enter the schema name (MASTER_REPOSITORY_USER) that you want to duplicate:")
				def source_schema = getInput("REPO_WORKS.duplicate_repository_with_postfix -- MASTER_REPO_USER >>");
				println("->Please enter the postfix to be added:")
				def postfix = getInput("REPO_WORKS.duplicate_repository_with_postfix -- POSTFIX >>");
				duplicate_repository_with_postfix(source_schema, postfix)
			} else if(raw_input.equalsIgnoreCase("c")){
				println("## This will copy tables, constraints and the data. Good old copy.")
				println("->Please enter the source schema name FROM which data will be copied:")
				def source_schema = getInput("REPO_WORKS.copy_repository -- FROM >>");
				println("->Please enter the target schema name TO which data will be copied:")
				def target_schema = getInput("REPO_WORKS.copy_repository -- TO >>");
				copy_repository(source_schema, target_schema)
			} else if(raw_input.equalsIgnoreCase("cr")){
				println("## This will create repository user with grants and packages.")
				println("->Please enter the schema name to create:")
				def username = getInput("REPO_WORKS.create_repo_user -- USERNAME >>");
				println("->What should be the password?:")
				def password = getInput("REPO_WORKS.create_repo_user -- PASSWORD >>");
				create_repo_user(username, password)
			} else if(raw_input.equalsIgnoreCase("r")){
				println("## This will only refresh the data. Tables must be copied beforehand. It disable the constraints.")
				println("->Please enter the source schema name FROM which data will be copied:")
				def source_schema = getInput("REPO_WORKS.refresh_repo_data -- FROM >>");
				println("->Please enter the target schema name TO which data will be copied:")
				def target_schema = getInput("REPO_WORKS.refresh_repo_data -- TO >>");
				refresh_repo_data(raw_input, raw_input)
			} else if(raw_input.equalsIgnoreCase("rl")){
				println("## This will refresh the data with the session logs. Just to be safe.")
				println("->Please enter the source schema name FROM which data will be copied:")
				def source_schema = getInput("REPO_WORKS.refresh_repo_data_with_logs -- FROM >>");
				println("->Please enter the target schema name TO which data will be copied:")
				def target_schema = getInput("REPO_WORKS.refresh_repo_data_with_logs -- TO >>");
				refresh_repo_data_with_logs(raw_input, raw_input)
			}
		}
	}

	static def test(){
	}

	static String getInput(String prefix){
		print(prefix)
		def raw_input = addToMemory(System.in.newReader().readLine());
		if(raw_input.equalsIgnoreCase("q")){
			println("quit...")
			System.exit(0);
		} else if((raw_input.equalsIgnoreCase("h"))||(raw_input.equalsIgnoreCase("help"))){
			printHelp();
		} else if(raw_input.equalsIgnoreCase("m")){
			printMemory();
		} else if(raw_input.equalsIgnoreCase("1")) {
			return mem[1]
		} else if(raw_input.equalsIgnoreCase("2")) {
			return mem[2]
		} else if(raw_input.equalsIgnoreCase("3")) {
			return mem[3]
		} else if(raw_input.equalsIgnoreCase("4")) {
			return mem[4]
		} else if(raw_input.equalsIgnoreCase("5")) {
			return mem[5]
		} else if(raw_input.equalsIgnoreCase("6")) {
			return mem[6]
		}
		return raw_input;
	}

	static void printHelp(){
		println("##########")
		println("## (h)\t - help")
		//println("## (m)\t - to print out memory")
		println("## (d)\t- duplicate_repository_with_postfix\t - works recursive")
		println("## (c)\t- copy_repository\t - simply copy 'A' to 'B'")
		println("## (cr)\t- create_repo_user\t - to create repo user")
		println("## (r)\t- refresh_repo_data\t - to refresh data of an existing user")
		println("## (rl)\t- refresh_repo_data_with_logs\t - to refresh data with session logs")
		println("##########")
	}
	static String addToMemory(String raw_input){
		def tmp = [raw_input];
		this.mem = tmp + this.mem
		return raw_input;
	}

	static def printMemory(){
		println("Printing last 10 inputs:")
		int i = 0;
		mem.each { it ->
			println("[" + i.toString() + "] - " + it)
			i++;
		}
	}

	static def duplicate_repository_with_postfix(String source_schema, String postfix){
		def target_schema = source_schema + postfix;
		UserStuff trg = copy_repository(source_schema, target_schema);

		def workRepoList = trg.getWorkRepos()
		workRepoList.each { it ->
			duplicate_repository_with_postfix(it, postfix)
		}
		return trg;
		
		//TopologyStuff trg_repo = change_work_repo_users_with_postfix(source_schema, postfix)
	}

	static def change_work_repo_users_with_postfix(String source_schema, String postfix){
		UserStuff trg = new UserStuff();
		trg.setDB_CONNECTION_INFO(DB_CONNECTION_INFO);
		trg.setUsername(source_schema + postfix);
		TopologyStuff topology_works = new TopologyStuff();
		topology_works.setREPO_CONNECTION_INFO(REPO_CONNECTION_INFO);
		
		return null;
	}
	
	static def copy_repository(String source_schema, String target_schema){
		def trg = create_repo_user(target_schema, target_schema)
		copy_repo_tables(source_schema, target_schema)
		copy_repo_indexes(source_schema, target_schema)
		copy_repo_constraints(source_schema, target_schema)
		refresh_repo_data(source_schema, target_schema)
		return trg;
	}

	static def UserStuff create_repo_user(String username, String password){
		UserStuff user_works = new UserStuff();
		user_works.setDB_CONNECTION_INFO(DB_CONNECTION_INFO);
		user_works.drop_user_if_exists(username)
		user_works.create_user(username, password)
		user_works.deploy_package()
		return user_works;
	}

	static def refresh_repo_data(String source_schema, String target_schema){
		UserStuff src = new UserStuff()
		src = src.setUsername(source_schema).setDB_CONNECTION_INFO(DB_CONNECTION_INFO);
		UserStuff trg = new UserStuff()
		trg = trg.setUsername(target_schema).setDB_CONNECTION_INFO(DB_CONNECTION_INFO);

		trg.disable_repo_constraints();
		trg.truncate_repo();
		src.grant_select_privilege(target_schema);
		trg.copy_repo_data(source_schema);
		trg.enable_repo_constraints();
		return trg;
	}
	static def refresh_repo_data_with_logs(String source_schema, String target_schema){
		UserStuff src = new UserStuff()
		src = src.setUsername(source_schema).setDB_CONNECTION_INFO(DB_CONNECTION_INFO);
		UserStuff trg = new UserStuff()
		trg = trg.setUsername(target_schema).setDB_CONNECTION_INFO(DB_CONNECTION_INFO);

		trg.disable_repo_constraints();
		trg.truncate_repo();
		src.grant_select_privilege(target_schema);
		trg.copy_repo_data_with_logs(source_schema);
		trg.enable_repo_constraints();
		return trg;
	}
	static def copy_repo_tables(String source_schema, String target_schema){
		UserStuff src = new UserStuff();
		src = src.setUsername(source_schema).setDB_CONNECTION_INFO(DB_CONNECTION_INFO);
		UserStuff trg = new UserStuff()
		trg = trg.setUsername(target_schema).setDB_CONNECTION_INFO(DB_CONNECTION_INFO);

		def tableList = src.getRepoTables();
		trg.deploy_tables(tableList);
		return trg;
	}
	static def copy_repo_constraints(String source_schema, String target_schema){
		UserStuff src = new UserStuff();
		src = src.setUsername(source_schema).setDB_CONNECTION_INFO(DB_CONNECTION_INFO);
		UserStuff trg = new UserStuff()
		trg = trg.setUsername(target_schema).setDB_CONNECTION_INFO(DB_CONNECTION_INFO);

		def constraintList = src.getRepoConstraints();
		println "got constraints: " + constraintList.size().toString();
		trg.deploy_constraints(constraintList);
		return trg;
	}
	static def copy_repo_indexes(String source_schema, String target_schema){
		UserStuff src = new UserStuff();
		src = src.setUsername(source_schema).setDB_CONNECTION_INFO(DB_CONNECTION_INFO);
		UserStuff trg = new UserStuff()
		trg = trg.setUsername(target_schema).setDB_CONNECTION_INFO(DB_CONNECTION_INFO);

		def indexList = src.getRepoIndexes(target_schema);
		println "got indexes: " + indexList.size().toString();
		trg.deploy_indexes(indexList);
		return trg;
	}
}


