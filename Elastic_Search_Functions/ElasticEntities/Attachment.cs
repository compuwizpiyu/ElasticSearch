using Nest;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSearchLibrary.ElasticEntities
{
    public class Attachment
    {
        public Attachment()
        {

        }


        public string Author { get; set; }


        public long ContentLength { get; set; }


        public string ContentType { get; set; }


        public DateTime Date { get; set; }


        public string Keywords { get; set; }


        public string Language { get; set; }


        public string Name { get; set; }


        public string Title { get; set; }


        public string Content { get; set; }

    }
}
