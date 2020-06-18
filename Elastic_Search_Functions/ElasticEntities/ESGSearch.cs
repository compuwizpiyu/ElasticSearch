using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSearchLibrary.ElasticEntities
{
    [ElasticsearchType(IdProperty = nameof(ESGId))]
    public class ESGSearch
    {
        public string ESGComments { get; set; }

        public string ESGNextSteps { get; set; }

        public int ESGId { get; set; }
    }
}
