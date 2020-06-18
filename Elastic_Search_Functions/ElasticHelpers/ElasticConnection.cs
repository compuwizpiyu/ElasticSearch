using ElasticSearchLibrary.ElasticEntities;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace ElasticSearchLibrary.ElasticHelpers
{
    public class ElasticConnection
    {
        public IndexName Index { get; set; }
        public ElasticClient Client { get; set; }
        public ElasticConnection(IndexName index = null)
        {
            this.Index = index;
        }
        public ElasticClient ConnectElastic()
        {
            ConnectionSettings connectionSettings = null;
            var node = new Uri(System.Configuration.ConfigurationManager.AppSettings["ElasticNode"].ToString());//new Uri("http://HKGWKS139406:9200"); //
            if (this.Index == null)
            {
                var documentsIndex = System.Configuration.ConfigurationManager.AppSettings["ElasticIndex"].ToString();
                this.Index = documentsIndex;
                connectionSettings = new ConnectionSettings(node)
                                    .DefaultMappingFor<Document>(m => m.IndexName(this.Index.Name)
                                    );
            }
            else
            {
                connectionSettings = new ConnectionSettings(node)
                .DefaultMappingFor<ESGSearch>(m => m.IndexName(this.Index.Name)
                    );
            }
            var client = new ElasticClient(connectionSettings);
            this.Client = client;
            return client;
        }

    }
}
