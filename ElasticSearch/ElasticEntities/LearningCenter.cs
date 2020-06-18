using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSearch.ElasticEntities
{
    public class LearningCenter
    {
        public int Id { get; set; }
        public int LearningDetailsId { get; set; }
        public string Path { get; set; }
        public string Content { get; set; }

        public string Title { get; set; }

        public string Author { get; set; }
        public string Headline { get; set; }
        
        public string DocumentType { get; set; }
        
        public string SectorName { get; set; }

        public string SectorCode { get; set; }       
       
        public DateTime? UploadDate { get; set; }
        public Attachment Attachment { get; set; }


    }
}
