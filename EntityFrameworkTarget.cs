
//
// Copyright (c) 2012 Jay Miller <jnmiller@cryptofreak.org>
//
// Additionally, this library was built using code from NLog, which is
// distributed under the following license and copyright:
// 
// Copyright (c) 2004-2011 Jaroslaw Kowalski <jaak@jkowalski.net>
// 
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without 
// modification, are permitted provided that the following conditions 
// are met:
// 
// * Redistributions of source code must retain the above copyright notice, 
//   this list of conditions and the following disclaimer. 
// 
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution. 
// 
// * Neither the name of Jaroslaw Kowalski nor the names of its 
//   contributors may be used to endorse or promote products derived from this
//   software without specific prior written permission. 
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
// THE POSSIBILITY OF SUCH DAMAGE.
// 

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.EntityClient;
using System.Diagnostics;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;

namespace NLog.EntityFramework
{
    /// <summary>
    /// An NLog target that writes logging messages using an ADO.NET provider that is configurable via an Entity Framework connection string.
    /// </summary>
    [Target("EntityFramework")]
    public class EntityFrameworkTarget : Target
    {
        /// <summary>
        /// Gets or sets the name of the connection string to be used for this log target.
        /// </summary>
        [RequiredParameter]
        public string ConnectionStringName { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to keep the database connection open between the log events.
        /// </summary>
        [DefaultValue(true)]
        public bool KeepConnection { get; set; }

        /// <summary>
        /// Gets or sets the text of the SQL command to be run on each log level.
        /// </summary>
        [RequiredParameter]
        public Layout CommandText { get; set; }

        /// <summary>
        /// Gets the collection of parameters.  Each parameter contains a mapping between NLog layout and a database named or positional parameter.
        /// </summary>
        [ArrayParameter(typeof(DatabaseParameterInfo), "parameter")]
        public IList<DatabaseParameterInfo> Parameters { get; private set; }

        private string ConnectionString { get; set; }
        private DbProviderFactory ProviderFactory { get; set; }

        private IDbConnection Database
        {
            get
            {
                if (_database == null)
                    _database = OpenDatabase(ProviderFactory, ConnectionString);
                return _database;
            }
        }
        private IDbConnection _database;

        /// <summary>
        /// Constructs a new Entity Framework log target.
        /// </summary>
        public EntityFrameworkTarget()
        {
            KeepConnection = true;
            Parameters = new List<DatabaseParameterInfo>();
        }

        /// <summary>
        /// Entry point from NLog.
        /// </summary>
        protected override void InitializeTarget()
        {
            base.InitializeTarget();

            ConnectionString = GetProviderConnectionString(ConnectionStringName);
            ProviderFactory = DbProviderFactories.GetFactory("System.Data.SqlClient");
        }

        /// <summary>
        /// Entry point from NLog.
        /// </summary>
        protected override void CloseTarget()
        {
            base.CloseTarget();
            CloseDatabase();
        }

        /// <summary>
        /// Entry point from NLog.
        /// </summary>
        /// <param name="logEvent">Event to write to the log.</param>
        protected override void Write(LogEventInfo logEvent)
        {
            try
            {
                WriteToDatabase(logEvent);
            }
            catch (Exception e)
            {
                Trace.WriteLine(String.Format("Error writing event to database: {0}\n{1}", e.Message, e.ToString()));
                CloseDatabase();
                throw;
            }
            finally
            {
                if (!KeepConnection)
                    CloseDatabase();
            }
        }

        #region Low-level database methods

        private void WriteToDatabase(LogEventInfo logEvent)
        {
            var command = Database.CreateCommand();
            command.CommandText = CommandText.Render(logEvent);

            Trace.WriteLine(String.Format("SQL {0} -> {1}", command.CommandType, command.CommandText));
            Trace.Indent();

            foreach (var paramInfo in Parameters)
            {
                var param = command.CreateParameter();

                if (paramInfo.Name != null)
                    param.ParameterName = paramInfo.Name;

                if (paramInfo.Size != 0)
                    param.Size = paramInfo.Size;
                if (paramInfo.Precision != 0)
                    param.Precision = paramInfo.Precision;
                if (paramInfo.Scale != 0)
                    param.Scale = paramInfo.Scale;

                param.Direction = ParameterDirection.Input;
                param.Value = paramInfo.Layout.Render(logEvent);
                command.Parameters.Add(param);

                Trace.WriteLine(String.Format("'{0}' = '{1}' ({2})", param.ParameterName, param.Value, param.DbType));
            }

            int result = command.ExecuteNonQuery();
            Trace.Unindent();
            Trace.WriteLine(String.Format("{0} row(s) affected.", result));
        }

        private static string GetProviderConnectionString(string connectionStringName)
        {
            var container = ConfigurationManager.ConnectionStrings[connectionStringName];
            if (container == null)
                throw new ConfigurationErrorsException(String.Format(
                        Strings.InvalidContainerErrorFormat, connectionStringName));

            var app_conn_string = container.ConnectionString;
            var ef_connection_settings = new EntityConnectionStringBuilder(app_conn_string);
            return ef_connection_settings.ProviderConnectionString;
        }

        private static IDbConnection OpenDatabase(DbProviderFactory factory, string connectionString)
        {
            var connection = factory.CreateConnection();
            connection.ConnectionString = connectionString;
            connection.Open();
            return connection;
        }

        private void CloseDatabase()
        {
            if (_database == null)
                return;

            _database.Close();
            _database = null;
        }

        #endregion
    }
}
