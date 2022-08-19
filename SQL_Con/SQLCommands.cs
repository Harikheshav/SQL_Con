using System;
using System.Collections;
using System.Data.SqlClient;
using System.Data;
namespace SQL_Con
{ 
/// <summary>
/// A Class Library for applying basic SQL Commands.
/// Takes the database name as a parameter.
/// </summary>
    public class SQLCommands
    {
        string databasename;
        public static SqlConnection con;
        public static SqlCommand cmd;
        private SqlConnection getcon()
        {
            SqlConnection con = new SqlConnection("Data Source=.;Initial Catalog=" + databasename + ";Integrated Security=true");
            con.Open();
            return con;
        }
        private string cmdstrval(Hashtable colfields)
        {
            string valstr = "";
            int i = 0;
            foreach (DictionaryEntry field in colfields)
            {
                valstr = valstr + field.Key;
                i++;
                if (i != colfields.Count)
                    valstr = valstr + ",";

            }
            valstr = valstr + ") values(@";
            i = 0;
            foreach (DictionaryEntry field in colfields)
            {
                valstr = valstr + field.Key;
                i++;
                if (i != colfields.Count)
                    valstr = valstr + ",@";
                else
                    valstr = valstr + ")";

            }
            return valstr;

        }
        private string cmdstrwhere(string wherecolname = "", string what = "",string op="=")
        {
            string cmdstr="";
            int num;
            if (int.TryParse(what, out num) || op=="in"||op=="not in")
                cmdstr += " where " + wherecolname + " " +op + what;
            else
                cmdstr += " where " + wherecolname + " " + op + "'" + what + "'";
            return cmdstr;
        }
        /// <summary>
        /// If a wrong SQL Command is executed please apply the following exception
        /// </summary>
        public class WrongSQLCommand : Exception
        {
            public WrongSQLCommand(string message) : base(message)
            {

            }
        }
        /// <summary>
        /// Constructor which takes the database handled by the library as a parameter. 
        /// </summary>
        /// <param name="databasename"></param>
        public SQLCommands(string databasename)
        {
            this.databasename = databasename;
        }
        /// <summary>
        ///Outputs the following command:select colname from tablename where wherecolname op what
        /// </summary>
        /// <param name="tablename">Takes the tablename as input, Its a required parameter</param>
        /// <param name="colname">Takes the column names as input, add ',' multiple columns: Its a optional parameter</param>
        /// <param name="wherecolname">Takes the column name required for condition, its an optional parameter</param>
        /// <param name="op">Takes the operator required for condition, its an optional parameter</param>
        /// <param name="what">Takes the value required to be compared with condition, its an optional parameter</param>
        /// <param name="wherewhat">In case the commands are big in wherecolname op what commands use wherewhat string</param>
        /// <param name="groupby">Groups the output as per a specific column</param>
        /// <param name="orderby">Orders the output as per a specific column</param>
        /// <param name="desc">If orderby is active order in desc</param>
        /// <returns>Returns a SQLDataReader object with all the output columns</returns>
        /// <exception cref="WrongSQLCommand">Incase the command created is invalid this exception occurs</exception>
        public SqlDataReader selectdata(string tablename, string colname = "*", string wherecolname = "", string op = "=", string what = "", string wherewhat = "", string groupby = "", string orderby = "",bool desc=false)
        //select colname from tablename where wherecolname op what
        //For bigger commands in wherecolname op what commands use wherewhat string
        {
            string cmdstr = "select " + colname + " from " + tablename;
            if (wherecolname != "" && what != "")
                cmdstr += cmdstrwhere(wherecolname, what, op);
            else
                cmdstr +=" " +wherewhat;
            if (groupby != "")
                cmdstr += " group by " + groupby;
            if (orderby != "")
                cmdstr += " order by " + orderby;
            if (desc)
                cmdstr += " desc";
            con = getcon();
            cmd = new SqlCommand(cmdstr, con);
            try
            {
                SqlDataReader dr = cmd.ExecuteReader();
                return dr;
            }
            catch(Exception ex)
            {
                throw new WrongSQLCommand("The SQL command:" + cmdstr+" is not valid due to "+ ex);
            }
        }
        //Only Select can be used in a method called disconnected architecture which has less refresh rates and gives 
        //the output of earlier records incase SQL Server is busy
        /// <summary>
        ///select colname from tablename where wherecolname op what
        ///Only Select can be used in a method called disconnected architecture which has less refresh rates and 
        ///gives the output of earlier records incase SQL Server is busy
        /// </summary>
        /// <param name="tablename">Takes the tablename as input, Its a required parameter</param>
        /// <param name="colname">Takes the column names as input, add ',' multiple columns: Its a optional parameter</param>
        /// <param name="wherecolname">Takes the column name required for condition, its an optional parameter</param>
        /// <param name="op">Takes the operator required for condition, its an optional parameter</param>
        /// <param name="what">Takes the value required to be compared with condition, its an optional parameter</param>
        /// <param name="wherewhat">In case the commands are big in wherecolname op what commands use wherewhat string</param>
        /// <param name="groupby">Groups the output as per a specific column</param>
        /// <param name="orderby">Orders the output as per a specific column</param>
        /// <param name="desc">If orderby is active order in desc</param>
        /// <returns>Returns a SQLDataReader object with all the output columns</returns>
        /// <exception cref="WrongSQLCommand">Incase the command created is invalid this exception occurs</exception>
        public DataTable selectdatadiscon(string tablename, string colname = "*", string wherecolname = "", string op = "=", string what = "", string wherewhat = "",string groupby="",string orderby="",bool desc=false)
        //select colname from tablename where wherecolname op what
        //For bigger commands in wherecolname op what commands use wherewhat string
        {
            string cmdstr = "select " + colname + " from " + tablename;
            if (wherecolname != "" && what != "")
                cmdstr += cmdstrwhere(wherecolname, what,op);
            else
                cmdstr += " " + wherewhat;
            if (groupby != "")
                cmdstr += " group by " + groupby;
            if (orderby != "")
                cmdstr += " order by " + orderby;
            if (desc)
                cmdstr += " desc";
            con = getcon();
            try
            {
                cmd = new SqlCommand(cmdstr, con);
                SqlDataAdapter da = new SqlDataAdapter(cmd);
                DataSet ds = new DataSet();
                da.Fill(ds);
                DataTable dt = ds.Tables[0];
                return dt;
            }
            catch (Exception ex)
            {
                throw new WrongSQLCommand("The SQL command:" + cmdstr + " is not valid due to " + ex);
            }
        }
        /// <summary>
        /// Takes either a SQL DataReader Object or a DataTable Object and converts into a 2D List.
        /// Any one value is optional, incase both are valid, gives a combined list.
        /// </summary>
        /// <param name="dr">SQL DataReader Object</param>
        /// <param name="dt">DataTable Object</param>
        /// <returns></returns>
        public List<List<string>> SQL_Lst(SqlDataReader dr=null,DataTable dt=null)
        {
            List<List<string>> dlst = new List<List<string>>();
            if (dr != null)
            {
                while (dr.Read())
                {
                    List<string> lst = new List<string>();
                    for (int i = 0; i < dr.FieldCount; i++)
                    {
                        lst.Add(dr[i].ToString());
                    }
                    dlst.Add(lst);
                }
                dr.Close();
            }
            if(dt!=null)
            {
                foreach(DataRow d in dt.Rows)
                {
                    List<string> lst = new List<string>();
                    foreach(var item in d.ItemArray)
                    {
                        lst.Add(item.ToString());
                    }
                    dlst.Add(lst);
                }
            }
            return dlst;
        }
        /// <summary>
        /// Takes a tablename in the database as input and gets all the column names and its column type as a Dictionary
        /// </summary>
        /// <param name="tablename">TableName</param>
        /// <returns>Dictionary:{columnname,columntype}</returns>
        public Dictionary<string, string> getcolname(string tablename)
        //select column_name,data_type from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME=tablename
        {
            SqlDataReader dr = selectdata("INFORMATION_SCHEMA.COLUMNS", "column_name,data_type", "TABLE_NAME", tablename);
            Dictionary<string, string> dct = new Dictionary<string, string>();
            while (dr.Read())
            {
                for (int i = 0; i < dr.FieldCount; i += 2)
                {
                    dct.Add(dr[i].ToString(), dr[i + 1].ToString());
                }
            }
            dr.Close();
            return dct;
        }
        /// <summary>
        /// insert into tablename(colfield1,colfield2,.....,colfieldn) values(@colfield1,@colfield2,.....,@colfieldn)
        /// </summary>
        /// <param name="tablename">Takes the tablename as input, Its a required parameter</param>
        /// <param name="colfields">Takes the column fields as a Hashtable object:{colfield1,@colfield1,.....}, Its a required parameter</param>
        /// <exception cref="WrongSQLCommand">Incase the command created is invalid this exception occurs</exception>
        public void insertdata(string tablename, Hashtable colfields)
        //insert into tablename(colfield1,colfield2,.....,colfieldn) values(@colfield1,@colfield2,.....,@colfieldn)
        {
            con = getcon();
            string cmdstri = "insert into "+tablename+"(";
            string valstr = cmdstrval(colfields);
            cmd = new SqlCommand(cmdstri+valstr,con);
            foreach (DictionaryEntry field in colfields)
                cmd.Parameters.AddWithValue("@" + field.Key, field.Value);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new WrongSQLCommand("The SQL command:" + cmdstri + valstr + "is not valid due to " + ex);
            }

        }
        /// <summary>
        /// delete from tablename where wherecolname op what
        ///For bigger commands in wherecolname op what commands use wherewhat string
        /// </summary>
        /// <param name="tablename">Takes the tablename as input, Its a required parameter</param>
        /// <param name="wherecolname">Takes the column name required for condition, its an optional parameter</param>
        /// <param name="op">Takes the operator required for condition, its an optional parameter</param>
        /// <param name="what">Takes the value required to be compared with condition, its an optional parameter</param>
        /// <param name="wherewhat">In case the commands are big in wherecolname op what commands use wherewhat string</param>
        /// <exception cref="WrongSQLCommand">Incase the command created is invalid this exception occurs</exception>
        public void deletedata(string tablename, string wherecolname = "", string op = "=", string what = "", string wherewhat = "")
        //delete from tablename where wherecolname op what
        //For bigger commands in wherecolname op what commands use wherewhat string
        {
            string cmdstr = "delete from " + tablename;
            if (wherecolname != "" && what != "")
                cmdstr += cmdstrwhere(wherecolname, what, op);
            else
                cmdstr += " " + wherewhat;
            con = getcon();
            try
            {
                cmd = new SqlCommand(cmdstr, con);
                cmd.ExecuteNonQuery();
            }
            catch(Exception ex)
            {
                throw new WrongSQLCommand("The SQL command:" + cmdstr + " is not valid due to " + ex);
            }
        }
        /// <summary>
        /// update table_name set colfield1=@colfield1,colfield2=@colfield2,.....colfieldn=@colfieldn where wherecolname op what
        ///For bigger commands in wherecolname op what commands use wherewhat string
        /// </summary>
        /// <param name="tablename">Takes the tablename as input, Its a required parameter</param>
        /// <param name="colfields">Takes the column fields as a Hashtable object:{colfield1,@colfield1,.....}, Its a required parameter</param>
        /// <param name="wherecolname">Takes the column name required for condition, its an optional parameter</param>
        /// <param name="op">Takes the operator required for condition, its an optional parameter</param>
        /// <param name="what">Takes the value required to be compared with condition, its an optional parameter</param>
        /// <param name="wherewhat">In case the commands are big in wherecolname op what commands use wherewhat string</param>
        /// <exception cref="WrongSQLCommand">Incase the command created is invalid this exception occurs</exception>
        public void updatedata(string tablename, Hashtable colfields, string wherecolname = "", string op = "=", string what = "",string wherewhat="")
        //update table_name set colfield1=@colfield1,colfield2=@colfield2,.....colfieldn=@colfieldn where wherecolname op what
        //For bigger commands in wherecolname op what commands use wherewhat string
        {
            string cmdstr = "update " + tablename + " set ";
            int i = 0, num_int;
            float num_flt;
            DateTime dt;
            foreach (DictionaryEntry field in colfields)
            {
                if (int.TryParse(field.Value.ToString(), out num_int) ||float.TryParse(field.Value.ToString(), out num_flt))
                {
                    cmdstr = cmdstr + field.Key + "=" + field.Value.ToString();
                }
                else if(DateTime.TryParse(field.Value.ToString(), out dt))
                {
                    cmdstr = cmdstr + field.Key + "=" + "'"+dt.ToString("yyyy-MM-dd")+"'";
                }
                else
                {
                    cmdstr = cmdstr + field.Key + "=" + "'"+field.Value.ToString()+"'";
                }
                i++;
                if (i != colfields.Count)
                    cmdstr = cmdstr + ",";
            }
            if (wherecolname != "" && what != "")
                cmdstr = cmdstr  + cmdstrwhere(wherecolname, what,op);
            else
                cmdstr += " " + wherewhat;
            con = getcon();
            try
            {
                cmd = new SqlCommand(cmdstr, con);
                cmd.ExecuteNonQuery();
            }
            catch(Exception ex)
            {
                throw new WrongSQLCommand("The SQL command:" + cmdstr + " is not valid due to " + ex);
            }
        }
    }
}