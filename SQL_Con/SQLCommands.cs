using System;
using System.Collections;
using System.Data.SqlClient;
using System.Data;
//Updates in Progress:
//Add function descriptions....
namespace SQL_Con
{ 
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
        public class WrongSQLCommand : Exception
        {
            public WrongSQLCommand(string message) : base(message)
            {

            }
        }
        public SQLCommands(string databasename)
        {
            this.databasename = databasename;
        }

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
        public void updatedata(string tablename, Hashtable colfields, string wherecolname = "", string op = "=", string what = "",string wherewhat="")
        {
            //update table_name set colfield1=@colfield1,colfield2=@colfield2,.....colfieldn=@colfieldn where wherecolname op what
            //For bigger commands in wherecolname op what commands use wherewhat string
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