using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSearch.ElasticEntities
{
    public class Document
    {
        public int Id { get; set; }
        public int DocId { get; set; }
        public string Path { get; set; }
        public string Content { get; set; }

        public string Title { get; set; }

        public string Author { get; set; }

        public DateTime? DateOfMeeting { get; set; }

        public string Location { get; set; }
        public string SectorName { get; set; }

        public string SectorCode { get; set; }

        public string RegionName { get; set; }

        public string RegionCode { get; set; }

        public string Country { get; set; }

        public string CountryCode { get; set; }
        public string CompanyName { get; set; }
        public int CompanyId { get; set; }
        public DateTime? UploadDate { get; set; }
        public Attachment Attachment { get; set; }

        public Document()
        { }
    }

}
