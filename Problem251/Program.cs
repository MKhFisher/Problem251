using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Problem251
{
    class Program
    {
        static SqlConnection m_conn = null;
        static SqlConnection m_conn_master = null;

        static void Main(string[] args)
        {
            m_conn = new SqlConnection(@"Server=68.231.212.53;Database=CS253;User Id=sa;Password=admin;");
            m_conn_master = new SqlConnection(@"Server=68.231.212.53;Database=master;User Id=sa;Password=admin;");

            if (!IsDBPresent(m_conn_master, "CS253"))
            {
                m_conn_master.Open();

                SqlCommand create_db = new SqlCommand("CREATE DATABASE CS253", m_conn_master);
                create_db.ExecuteNonQuery();

                m_conn_master.Close();
            }
            
            if (!IsTablePresent(m_conn, "documents"))
            {
                CreateDBSchema(m_conn);
                LoadFileIntoDatabase(args[0], m_conn);
            }

            QuerySQL(m_conn);
        }

        public static void QuerySQL(SqlConnection m_conn)
        {
            m_conn.Open();

            SqlCommand top25 = new SqlCommand("SELECT TOP 25 value, COUNT(*) AS frequency FROM words GROUP BY value ORDER BY COUNT(*) DESC", m_conn);
            using (SqlDataReader dr = top25.ExecuteReader())
            {
                while (dr.Read())
                {
                    Console.WriteLine("{0}  -  {1}", dr.GetString(0), dr.GetInt32(1));
                }
            }

            m_conn.Close();
        }

        public static bool IsTablePresent(SqlConnection m_conn, string table)
        {
            bool result;
            m_conn.Open();

            SqlCommand tb_exists = new SqlCommand("SELECT COUNT(*) FROM information_schema.tables WHERE Table_Name = '" + table + "'", m_conn);
            int tb_id;
            try
            {
                tb_id = (int)tb_exists.ExecuteScalar();
            }
            catch
            {
                tb_id = -1;
            }

            m_conn.Close();
            return result = (tb_id > 0);
        }

        public static bool IsDBPresent(SqlConnection m_conn, string database)
        {
            bool result;
            m_conn.Open();
            
            SqlCommand db_exists = new SqlCommand("SELECT database_id FROM sys.databases WHERE Name = '" + database + "'", m_conn);
            int db_id;
            try
            {
                db_id = (int)db_exists.ExecuteScalar();
            }
            catch
            {
                db_id = -1;
            }

            m_conn.Close();
            return result = (db_id > 0);
        }

        public static void CreateDBSchema(SqlConnection m_conn)
        {
            m_conn.Open();

            SqlCommand create_documents = new SqlCommand("CREATE TABLE documents (id int IDENTITY(1,1) PRIMARY KEY, name varchar(50))", m_conn);
            create_documents.ExecuteNonQuery();

            SqlCommand create_words = new SqlCommand("CREATE TABLE words (id int, doc_id int, value varchar(50))", m_conn);
            create_words.ExecuteNonQuery();

            SqlCommand create_characters = new SqlCommand("CREATE TABLE characters (id int, word_id int, value varchar(1))", m_conn);
            create_characters.ExecuteNonQuery();

            m_conn.Close();
        }

        public static void LoadFileIntoDatabase(string file, SqlConnection m_conn)
        {
            int doc_id;
            int word_id;
            int char_id;
            string data = new StreamReader(file).ReadToEnd().Replace("_", " ").ToLower();
            List<string> text = Regex.Split(data, "\\W+").ToList();
            HashSet<string> stopwords = new HashSet<string>(new StreamReader("stop_words.txt").ReadToEnd().Replace(",\n\n", "").Split(','));
            List<string> words = new List<string>();

            foreach (string word in text)
            {
                if (!stopwords.Contains(word) && word != "s")
                {
                    words.Add(word);
                }
            }

            m_conn.Open();

            SqlCommand insert_doc = new SqlCommand("INSERT INTO documents (name) VALUES (@name)", m_conn);
            insert_doc.Parameters.AddWithValue("@name", file);
            insert_doc.ExecuteNonQuery();

            doc_id = GetMaxDocID(m_conn);
            word_id = GetMaxWordOrCharID(m_conn, "words");
            char_id = GetMaxWordOrCharID(m_conn, "characters");

            foreach (string word in words)
            {
                SqlCommand insert_words = new SqlCommand("INSERT INTO words (id, doc_id, value) VALUES (@id, @doc_id, @value)", m_conn);
                insert_words.Parameters.AddWithValue("@id", word_id);
                insert_words.Parameters.AddWithValue("@doc_id", doc_id);
                insert_words.Parameters.AddWithValue("@value", word);
                insert_words.ExecuteNonQuery();

                foreach (char c in word)
                {
                    SqlCommand insert_chars = new SqlCommand("INSERT INTO characters (id, word_id, value) VALUES (@id, @word_id, @value)", m_conn);
                    insert_chars.Parameters.AddWithValue("@id", char_id);
                    insert_chars.Parameters.AddWithValue("@word_id", word_id);
                    insert_chars.Parameters.AddWithValue("@value", c);
                    insert_chars.ExecuteNonQuery();

                    char_id++;
                }
                word_id++;
            }

            m_conn.Close();
        }

        public static int GetMaxDocID(SqlConnection m_conn)
        {
            SqlCommand result_id = new SqlCommand("SELECT TOP 1 id FROM documents ORDER BY id DESC", m_conn);
            int result = (int)result_id.ExecuteScalar();

            return result;
        }

        public static int GetMaxWordOrCharID(SqlConnection m_conn, string table)
        {
            SqlCommand result_id = new SqlCommand("SELECT TOP 1 id FROM " + table + " ORDER BY id DESC", m_conn);
            int result = 1;
            try
            {
                result = (int)result_id.ExecuteScalar();
            }
            catch { }

            return result;
        }
    }
}
