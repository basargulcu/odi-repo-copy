package odi_repo_copy.user
import java.lang.Class
import java.sql.DriverManager
import groovy.sql.Sql

class UserStuff {

	def DB_CONNECTION_INFO;
	def String username;
	def String password;

	public String getUsername() {
		return username;
	}
	public UserStuff setUsername(String username) {
		this.username = username.toUpperCase();
		return this;
	}
	public String getPassword() {
		return password;
	}
	public UserStuff setPassword(String password) {
		this.password = password;
		return this;
	}
	def drop_user_if_exists(String user_name){
		java.lang.Class.forName(DB_CONNECTION_INFO['driver'])
		def jdbc = DriverManager.getConnection(DB_CONNECTION_INFO['url'], DB_CONNECTION_INFO['db_user'], DB_CONNECTION_INFO['db_pass'])
		def stmt = jdbc.createStatement()
		def dyn_task = 	sprintf('SELECT COUNT(*) cnt FROM all_users WHERE 1 = 1 AND username = \'%1$s\'',	[user_name]);
		println(dyn_task)
		def rs = stmt.executeQuery(dyn_task)
		int cnt = 0
		while(rs.next()){
			cnt = rs.getInt(1)
		}
		stmt.close();

		if(cnt == 1){
			stmt = jdbc.createStatement()
			dyn_task = 	sprintf('drop user %1$s cascade',	[user_name]);
			println(dyn_task)
			def result = stmt.executeQuery(dyn_task)
			stmt.close();
		}
		jdbc.commit();
		jdbc.close();
		println("Done")
	}
	def create_user(String user_name, String pass){
		setUsername(user_name);
		setPassword(pass);
		java.lang.Class.forName(DB_CONNECTION_INFO['driver'])
		def jdbc = DriverManager.getConnection(DB_CONNECTION_INFO['url'], DB_CONNECTION_INFO['db_user'], DB_CONNECTION_INFO['db_pass'])
		def stmt = jdbc.createStatement()
		def dyn_task = 	sprintf('create user %1$s identified by %2$s default tablespace USERS  temporary tablespace TEMP',	[user_name, pass]);
		println(dyn_task)
		def result = stmt.executeQuery(dyn_task)
		stmt.close();


		def grant_list = ['grant connect, resource to %1$s'
			, 'grant create table, create session, create sequence to %1$s'
			, 'GRANT CREATE PROCEDURE, CREATE PUBLIC SYNONYM, CREATE SYNONYM, CREATE TRIGGER, CREATE TYPE  to %1$s'
			, 'ALTER USER %1$s QUOTA UNLIMITED ON USERS']

		for(String grant : grant_list){
			stmt = jdbc.createStatement()
			dyn_task = 	sprintf(grant,	[user_name]);
			println(dyn_task)
			result = stmt.executeQuery(dyn_task)
			stmt.close();
		}
		jdbc.commit();
		jdbc.close();
		println("Done")
	}

	def deploy_package(){
		if(username.equals("")){
			return false;
		}
		def sql = Sql.newInstance(DB_CONNECTION_INFO['url'], username, password, DB_CONNECTION_INFO['driver'])
		String dyn_task = new File('repo_works/spec').text
		sql.execute(dyn_task);
		println("SPEC deployed")
		dyn_task = new File('repo_works/body').text
		sql.execute(dyn_task);
		println("BODY deployed")
		sql.execute("grant execute on pkg_repo_works to " + DB_CONNECTION_INFO["db_user"] + " with grant option")
		println("Done")
		sql.connection.close();
	}

	public UserStuff setDB_CONNECTION_INFO(def dB_CONNECTION_INFO) {
		DB_CONNECTION_INFO = dB_CONNECTION_INFO;
		return this;
	}

	//baska schema bu user'a select hakki vermeli
	def grant_select_privilege(String target_schema){
		exec_function('grant_select_privilege', username, '\'' + target_schema + '\'')
	}
	def copy_repo_data(String source_schema){
		exec_function('copy_repo_data', '\'' + source_schema + '\', 0')
	}
	def copy_repo_data_with_logs(String source_schema){
		exec_function('copy_repo_data', '\'' + source_schema + '\', 1')
	}
	def enable_repo_constraints(){
		exec_function('enable_repo_constraints', '0')
	}
	def enable_repo_constraints_with_validation(){
		exec_function('enable_repo_constraints', '1')
	}
	def disable_repo_constraints(){
		exec_function('disable_repo_constraints', '')
	}
	def truncate_repo(){
		exec_function('truncate_tables', '')
	}
	def exec_function(String task, String parameterString){
		if(username.equals("")){
			return false;
		}
		exec_function(task, username, parameterString)
	}

	def exec_function(String task, String executer, String parameterString){
		if(username.equals("")){
			return false;
		}
		def sql = Sql.newInstance(DB_CONNECTION_INFO['url'], DB_CONNECTION_INFO['db_user'], DB_CONNECTION_INFO['db_pass'], DB_CONNECTION_INFO['driver']);
		def dyn_task = "begin ";
		dyn_task += executer + ".pkg_repo_works." + task + "("+ parameterString + "); ";
		dyn_task += "end; ";
		println(dyn_task)
		sql.execute(dyn_task);
		sql.connection.close();
	}
	def List<String> getRepoTables(){
		def tableList = []

		def sql = Sql.newInstance(DB_CONNECTION_INFO['url'], DB_CONNECTION_INFO['db_user'], DB_CONNECTION_INFO['db_pass'], DB_CONNECTION_INFO['driver']);
		sql.eachRow("SELECT 'create table ' || table_name || ' (' || joined_col_lines || ')' create_table "+
				"      FROM (  SELECT owner, table_name, listagg(col_line, ', ') WITHIN GROUP (order by column_id) joined_col_lines "+
				"                FROM (SELECT owner "+
				"                           , table_name "+
				"                           , column_name "+
				"                           , data_type "+
				"                           , data_length "+
				"                           , data_precision "+
				"                           , column_id "+
				"                           , CASE "+
				"                               WHEN data_type = 'LONG RAW' THEN column_name || ' BLOB' "+
				"                               WHEN data_type = 'VARCHAR2' THEN column_name || ' VARCHAR2(' || data_length || ')' "+
				"                               WHEN data_type = 'NUMBER' THEN column_name || ' NUMBER(' || data_precision || ')' "+
				"                               WHEN data_type IN ('DATE', 'CLOB', 'BLOB') THEN column_name || ' ' || data_type "+
				"                               ELSE column_name || ' ' || data_type || '(' || data_length || ')' "+
				"                             END col_line "+
				"                        FROM all_tab_columns "+
				"                       WHERE 1 = 1 "+
				"                         AND owner = '" + this.username + "' "+
				"                         AND table_name LIKE 'SNP%' "+
				"                         AND table_name NOT LIKE '%_BCK' "+
				"                         AND table_name NOT LIKE '%_BAK' "+
				"                         AND table_name NOT LIKE '%_YDK') "+
				"                    GROUP BY owner, table_name)"){ tableList << it.create_table; }
		sql.connection.close();
		return tableList;
	}

	def deploy_tables(List<String> createTableStatements){
		def sql = Sql.newInstance(DB_CONNECTION_INFO['url'], DB_CONNECTION_INFO['db_user'], DB_CONNECTION_INFO['db_pass'], DB_CONNECTION_INFO['driver']);
		createTableStatements.each { it ->
			def dyn_task = it.replace("create table ", "create table " + this.username + ".")
			try {
				sql.execute(dyn_task);
				println("created - " + dyn_task)
			} catch(Exception e) {
				println("table exists - " + dyn_task)
				//e.printStackTrace()
			}
		}
		sql.connection.close();
	}

	def List<java.lang.Object> getRepoConstraints(){
		def constraintList = []

		def sql = Sql.newInstance(DB_CONNECTION_INFO['url'], DB_CONNECTION_INFO['db_user'], DB_CONNECTION_INFO['db_pass'], DB_CONNECTION_INFO['driver']);
		sql.eachRow("WITH T1 AS " +
				"         (  SELECT C.OWNER " +
				"                 , C.R_CONSTRAINT_NAME " +
				"                 , C.CONSTRAINT_NAME " +
				"                 , C.CONSTRAINT_TYPE " +
				"                 , C.TABLE_NAME " +
				"                 , WM_CONCAT(CC.COLUMN_NAME) COL_LIST " +
				"              FROM ALL_CONSTRAINTS C, ALL_CONS_COLUMNS CC " +
				"             WHERE 1 = 1 " +
				"               AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME " +
				"               AND C.OWNER = CC.OWNER " +
				"               AND C.CONSTRAINT_TYPE IN ('C' " +
				"                                       , 'P' " +
				"                                       , 'U' " +
				"                                       , 'R') " +
				"               AND C.OWNER = '"+ this.username + "' " +
				"               AND C.TABLE_NAME LIKE 'SNP%' AND C.TABLE_NAME NOT LIKE '%_BCK' AND C.TABLE_NAME NOT LIKE '%_BAK' AND C.TABLE_NAME NOT LIKE '%_YDK' " +
				"          GROUP BY C.OWNER " +
				"                 , C.R_CONSTRAINT_NAME " +
				"                 , C.CONSTRAINT_NAME " +
				"                 , C.CONSTRAINT_TYPE " +
				"                 , C.TABLE_NAME) " +
				"  SELECT A.CONSTRAINT_TYPE " +
				"       , A.TABLE_NAME " +
				"       , A.COL_LIST " +
				"       , A.CONSTRAINT_NAME " +
				"       , B.TABLE_NAME R_TABLE_NAME " +
				"       , B.COL_LIST R_COL_LIST " +
				"    FROM T1 A, T1 B " +
				"   WHERE 1 = 1 AND A.R_CONSTRAINT_NAME = B.CONSTRAINT_NAME(+) AND A.OWNER = B.OWNER(+) " +
				"ORDER BY CASE WHEN A.CONSTRAINT_TYPE = 'C' THEN 1 WHEN A.CONSTRAINT_TYPE = 'P' THEN 2 WHEN A.CONSTRAINT_TYPE = 'U' THEN 3 WHEN A.CONSTRAINT_TYPE = 'R' THEN 4 ELSE 99 END "){
					def rec = ['constraint_type' : it.CONSTRAINT_TYPE
						, 'table_name' : it.TABLE_NAME
						, 'col_list' : it.COL_LIST?.asciiStream.text
						, 'constraint_name' : it.CONSTRAINT_NAME]
					if(it.R_TABLE_NAME != null){
						rec = rec + ['r_table_name' : it.R_TABLE_NAME
							, 'r_col_list' : it.R_COL_LIST?.asciiStream.text]
					}
					constraintList << rec
				}

		sql.connection.close();
		return constraintList;
	}
	def deploy_constraints(List<java.lang.Object> createConstraintStatements){
		def sql = Sql.newInstance(DB_CONNECTION_INFO['url'], DB_CONNECTION_INFO['db_user'], DB_CONNECTION_INFO['db_pass'], DB_CONNECTION_INFO['driver']);
		createConstraintStatements.each { it ->
			try {
				def dyn_task = "";
				if(it['constraint_type'].equals("C")){
					dyn_task = "ALTER TABLE " + this.username + "." + it['table_name'] + " MODIFY " + it['col_list'] + " NOT NULL ENABLE VALIDATE"
					sql.execute(dyn_task);
					println(dyn_task);
				} else if(it['constraint_type'].equals("P")){
					dyn_task = "ALTER TABLE " + this.username + "." + it['table_name'] + " ADD CONSTRAINT " + it['constraint_name'] + " PRIMARY KEY (" + it['col_list'] + ") USING INDEX " + this.username + "." + it['constraint_name'] + " ENABLE"
					sql.execute(dyn_task);
					println(dyn_task);
				} else if(it['constraint_type'].equals("U")){
					dyn_task = "ALTER TABLE " + this.username + "." + it['table_name'] + " ADD CONSTRAINT " + it['constraint_name'] + " UNIQUE (" + it['col_list'] + ") USING INDEX " + this.username + "." + it['constraint_name'] + " ENABLE"
					sql.execute(dyn_task);
					println(dyn_task);
				} else if(it['constraint_type'].equals("R")){
					dyn_task = "ALTER TABLE " + this.username + "." + it['table_name'] + " ADD (CONSTRAINT " + it['constraint_name'] + " FOREIGN KEY (" + it['col_list'] + ") REFERENCES " + this.username + "." + it['r_table_name'] + " (" + it['r_col_list'] + ") ENABLE VALIDATE)"
					sql.execute(dyn_task);
					println(dyn_task);
				}
			} catch(Exception e) {
				println("err - " + it['constraint_name'])
				//e.printStackTrace()
			}
		}
		sql.connection.close();
	}

	def List<java.lang.Object> getRepoIndexes(String target_schema){
		def indexList = []

		def sql = Sql.newInstance(DB_CONNECTION_INFO['url'], DB_CONNECTION_INFO['db_user'], DB_CONNECTION_INFO['db_pass'], DB_CONNECTION_INFO['driver']);
		sql.eachRow("WITH SRC AS                                                                                  " +
				"             (  SELECT I.OWNER                                                                   " +
				"                     , I.INDEX_NAME                                                              " +
				"                     , I.TABLE_NAME                                                              " +
				"                     , I.UNIQUENESS                                                              " +
				"                     , WM_CONCAT(II.COLUMN_NAME) COL_LIST                                        " +
				"                  FROM ALL_INDEXES I, ALL_IND_COLUMNS II                                         " +
				"                 WHERE 1 = 1                                                                     " +
				"                   AND I.OWNER = II.INDEX_OWNER                                                  " +
				"                   AND I.INDEX_NAME = II.INDEX_NAME                                              " +
				"                   AND I.TABLE_NAME = II.TABLE_NAME                                              " +
				"                   AND I.OWNER = '" + this.username + "'                                                  " +
				"                   AND I.TABLE_NAME LIKE 'SNP%'                                                  " +
				"                   AND I.TABLE_NAME NOT LIKE '%_BCK'                                             " +
				"                   AND I.TABLE_NAME NOT LIKE '%_BAK'                                             " +
				"                   AND I.TABLE_NAME NOT LIKE '%_YDK'                                             " +
				"              GROUP BY I.OWNER                                                                   " +
				"                     , I.INDEX_NAME                                                              " +
				"                     , I.TABLE_NAME                                                              " +
				"                     , I.UNIQUENESS)                                                             " +
				"             SELECT SRC.OWNER                                                                   " +
				"                   , SRC.INDEX_NAME                                                              " +
				"                   , SRC.TABLE_NAME                                                              " +
				"                   , SRC.UNIQUENESS                                                              " +
				"                   , SRC.COL_LIST FROM SRC                                                               " ){
					def rec = ['uniqueness' : it.UNIQUENESS
						, 'table_name' : it.TABLE_NAME
						, 'col_list' : it.COL_LIST?.asciiStream.text
						, 'index_name' : it.INDEX_NAME]
					indexList << rec
				}

		sql.connection.close();
		return indexList;
	}

	//loop un icini duzeltmedim henuz
	def deploy_indexes(List<java.lang.Object> createIndexStatements){
		def sql = Sql.newInstance(DB_CONNECTION_INFO['url'], DB_CONNECTION_INFO['db_user'], DB_CONNECTION_INFO['db_pass'], DB_CONNECTION_INFO['driver']);
		createIndexStatements.each { it ->
			try {
				def dyn_task = "CREATE "+ (it['uniqueness'].equals("UNIQUE") ? "UNIQUE" : "") + " INDEX " + this.username + "." + it['index_name'] + " ON " + this.username + "." + it['table_name'] + " (" + it['col_list'] + ")"
				sql.execute(dyn_task);
				println(dyn_task);
			} catch(Exception e) {
				println("err - " + it['index_name'])
				//e.printStackTrace()
			}
		}
		sql.connection.close();
	}


	def getWorkRepos(){
		def repoUserList = []

		def sql = Sql.newInstance(DB_CONNECTION_INFO['url'], DB_CONNECTION_INFO['db_user'], DB_CONNECTION_INFO['db_pass'], DB_CONNECTION_INFO['driver']);
		def isWorkRepo;
		try {
			sql.eachRow("SELECT count(1) cnt " +
					"           FROM all_tables "+
					"          WHERE 1 = 1 AND OWNER='"+ this.username +"' AND table_name = 'SNP_REM_REP'"){ isWorkRepo = it.cnt; }
		} catch (Exception e) {
			println("couldn't figure out if it's master or work repo!!")
		}
		//this is not a master repo
		if(isWorkRepo == 0) {
			return repoUserList;
		}

		//def sql = Sql.newInstance(DB_CONNECTION_INFO['url'], DB_CONNECTION_INFO['db_user'], DB_CONNECTION_INFO['db_pass'], DB_CONNECTION_INFO['driver']);
		try {
			sql.eachRow("SELECT CONN.USER_NAME " +
					"           FROM " + this.username + ".SNP_REM_REP REPO, " + this.username + ".SNP_CONNECT CONN"+
					"          WHERE 1 = 1 AND REPO.I_CONNECT = CONN.I_CONNECT"){ repoUserList << it.USER_NAME; }
		} catch (Exception e) {
			println("this is not a master repository")
		}
		return repoUserList;
	}
}
