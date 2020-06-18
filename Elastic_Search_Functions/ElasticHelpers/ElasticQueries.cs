using ElasticSearchLibrary.ElasticEntities;
using EMDBNetData.DataLayer;
using Nest;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSearchLibrary.ElasticHelpers
{
    public class ElasticQueries
    {
        private string CallReportPath = ConfigurationManager.AppSettings["CallReportDownloadPath"].ToString();
        public DLCallReportSearch dLCallReportSearch = new DLCallReportSearch();
        public ElasticClient ElasticClient { get; set; }
        public IndexName ElasticIndex { get; set; }
        public ElasticQueries(IndexName index= null)
        {
            var connection = new ElasticConnection(index);
            this.ElasticClient = connection.ConnectElastic();
            this.ElasticIndex = connection.Index;

        }

        public void CreateIndex(IndexName documentsIndex = null)
        {
            var index = documentsIndex == null ? this.ElasticIndex : documentsIndex;
            var client = this.ElasticClient;
            var indexResponse = client.CreateIndex(index, c => c
                                         .Settings(s => s
                                        .Analysis(a => a
                      .Analyzers(ad => ad
                        .Custom("windows_path_hierarchy_analyzer", ca => ca
                          .Tokenizer("windows_path_hierarchy_tokenizer")
                        )
                      )
                      .Tokenizers(t => t
                        .PathHierarchy("windows_path_hierarchy_tokenizer", ph => ph
                          .Delimiter('\\')
                        )
                      )
                    )
                  )
                  .Mappings(m => m
                    .Map<Document>(mp => mp
                      .AutoMap()
                      .AllField(all => all
                        .Enabled(false)
                      )
                      .Properties(ps => ps
                        .Text(s => s
                          .Name(n => n.Path)
                          .Analyzer("windows_path_hierarchy_analyzer")
                        )
                        .Object<ElasticEntities.Attachment>(a => a
                          .Name(n => n.Attachment)
                          .AutoMap()
                        )
                      )
                    )
                  )
                );
            client.PutPipeline("attachments", p => p
                  .Description("Document attachment pipeline")
                  .Processors(pr => pr
                    .Attachment<Document>(a => a
                      .Field(f => f.Content)
                      .TargetField(f => f.Attachment)
                    )
                    .Remove<Document>(r => r
                      .Field(f => f.Content)
                    )
                  )
                );
            PopulateIndex();
        }

        private void PopulateIndex()
        {
            var directory = @"\\sprsvr108833\CallReport\";//System.Configuration.ConfigurationManager.AppSettings["CallReportPath"].ToString();
            var callReportsCollection = Directory.GetFiles(directory, "*.docx");
            callReportsCollection.ToList().AddRange(Directory.GetFiles(directory, "*.doc"));
            int i = 0;
            var callReportElasticDataSet = dLCallReportSearch.GetCallReportDetailsForElastic();
            Parallel.ForEach(callReportsCollection, callReport =>
            {
                var base64File = Convert.ToBase64String(File.ReadAllBytes(callReport));
                var fileSavedName = callReport.Replace(directory, "");
                // var dt = dLCallReportSearch.GetCallFileName(fileSavedName.Replace("'", "''"));//replace the ' in a file name with '';
                var rows = callReportElasticDataSet.Select("CALL_SAVE_FILE = '" + fileSavedName.Replace("'", "''") + "'");
                if (rows != null && rows.Count() > 0)
                {
                    foreach (DataRow row in rows)
                    {
                        i++;
                        this.ElasticClient.Index(new Document
                        {
                            Id = i,
                            Path = row["CALL_SAVE_FILE"].ToString().Replace(CallReportPath, ""),
                            Title = row["CALL_FILE"].ToString(),
                            Author = row["USER_NAME"].ToString(),
                            DateOfMeeting = string.IsNullOrEmpty(row["CALL_DT"].ToString()) ? (DateTime?)null : Convert.ToDateTime(row["CALL_DT"].ToString()),
                            Location = row["CALL_FILE"].ToString().Replace(CallReportPath, ""),
                            UploadDate = string.IsNullOrEmpty(row["CALL_DT"].ToString()) ? (DateTime?)null : Convert.ToDateTime(row["CALL_REPORT_DT"].ToString()),
                            Content = base64File
                        }, p => p.Pipeline("attachments"));
                    }
                }
            });
        }

        public void UploadNewAttachmentData(string filePath)
        {
            var base64File = Convert.ToBase64String(File.ReadAllBytes(filePath));
            this.ElasticClient.Index(new Document
            {
                Id = new Random(35000).Next(),
                Path = filePath,
                Content = base64File
            }, p => p.Pipeline("attachments"));
        }

        public void UploadCallRptDoc(Document document)
        {
            try
            {
                this.ElasticClient.Index(document, p => p.Pipeline("attachments"));
            }
            catch(Exception ex)
            {
                throw ex;
            }
            
        }

        /// <summary>
        /// Load suggestions when user types min 3 letters in the UI autocomplete textbox. Suggestions will be loaded for author, company name, sector, region, country for Call report search page and comments and Next Steps for ESG Search page
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public ISearchResponse<T> ElasticSuggestion<T>(string query, int id=0) where T:class
        {
            ISearchResponse<T> suggestions = null;
            if (id==0)
            {
                
                suggestions = (ISearchResponse<T>)this.ElasticClient.Search<Document>(
                               x => x.From(0).Size(15)
                               .Query(q => q
                                        .MultiMatch(m => m
                                            .Query(query)
                                            .Fields(f => f
                                                            .Field(_ => _.Author)
                                                            .Field(_ => _.CompanyName)
                                                            .Field(_ => _.Country)
                                                            .Field(_ => _.SectorName)
                                                            .Field(_ => _.RegionName)
                                                            .Field(_ => _.Location))))
                                             .Highlight(
                                                h => h.Fields(
                                                    f => f.Field(p => p.Author),
                                                    f => f.Field(p => p.CompanyName),
                                                    f => f.Field(p => p.Country),
                                                    f => f.Field(p => p.SectorName),
                                                    f => f.Field(p => p.RegionName),
                                                    f => f.Field(p => p.Location)
                                                    )));
                
            }
            else
            {
               if(id==1)//search comments
                {
                    suggestions = (ISearchResponse<T>)this.ElasticClient.Search<ESGSearch>(
                              x => x.From(0).Size(20)
                              .Query(q => q
                                       .MultiMatch(m => m
                                           .Query(query)
                                           .Fields(f => f
                                                           .Field(_ => _.ESGComments)
                                                           )))
                                            .Highlight(
                                               h => h.Fields(
                                                   f => f.Field(p => p.ESGComments)

                                                   )));
                }
               if(id==2) //search next steps
                {
                    suggestions = (ISearchResponse<T>)this.ElasticClient.Search<ESGSearch>(
                             x => x.From(0).Size(20)
                             .Query(q => q
                                      .MultiMatch(m => m
                                          .Query(query)
                                          .Fields(f => f
                                                          .Field(_ => _.ESGNextSteps)
                                                          )))
                                           .Highlight(
                                              h => h.Fields(
                                                  f => f.Field(p => p.ESGNextSteps)

                                                  )));
                }

            }
            return (ISearchResponse<T>)suggestions;
        }
        /// <summary>
        /// Elastic search for the keywords entered with filter and sort parameters
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="pageNum"></param>
        /// <param name="sortField"></param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public ISearchResponse<T> ElasticSearch<T>(string query, int pageNum = 0, string sortField = "", string filter = "") where T : class
        {
            var startIndex = 10 * (pageNum - 1);
            ISearchResponse<Document> searchResults;


            if (string.IsNullOrEmpty(filter))
            {
                searchResults = this.ElasticClient.Search<Document>(s => s.From(startIndex).Size(10).Query(q => q.Bool(b => b
.Must(sh => sh
  .MultiMatch(mp => mp
  .Query(query).MinimumShouldMatch(MinimumShouldMatch.Percentage(50))
  .Fields(f => f.Field(f1 => f1.Attachment.Content).Field(f1 => f1.Author).Field(f1 => f1.Title).Field(f1 => f1.Location).Field(f1 => f1.CompanyName).Field(f1 => f1.SectorName).Field(f1 => f1.RegionName).Field(f1 => f1.Country))))
.Should(sh => sh
  .MultiMatch(mp => mp
  .Query(query).Type(TextQueryType.Phrase).Slop(50).MinimumShouldMatch(MinimumShouldMatch.Percentage(75))
  .Fields(f => f.Field(f1 => f1.Attachment.Content).Field(f1 => f1.Author).Field(f1 => f1.Title).Field(f1 => f1.Location).Field(f1 => f1.CompanyName).Field(f1 => f1.SectorName).Field(f1 => f1.RegionName).Field(f1 => f1.Country))))

  ))
  //.Query(q => q.Bool(b => b
  //.Should(sh => sh
  //.MultiMatch(mp => mp
  //.Query(query)
  //.Fields(f => f.Field(f1 => f1.Attachment.Content).Field(f1 => f1.Author).Field(f1 => f1.Title).Field(f1 => f1.Location).Field(f1 => f1.CompanyName))))))
  .Sort(so => so.Field(fi => SortFromUserInput(fi, sortField))) //.Scroll("1m")
                                                                //.Fields(f => f.Fields(f1 => f1.Attachment.Content, f2 => f2.Title, f4=>f4.Author, f5=>f5.CompanyName, f6=>f6.DateOfMeeting, f7=>f7.UploadDate)))).Sort(so=>so.Field(fi=>SortFromUserInput(fi,""))) //.Scroll("1m")
  .Highlight(h => h.PreTags("<b style='color:orange'>")
          .PostTags("</b>")
          .Fields
          (
           //f=>f.Field("*").FragmentSize(150).NumberOfFragments(3)
           f => f.Field(e => e.Attachment.Content).FragmentSize(150).NumberOfFragments(3).NoMatchSize(50)
          , f => f.Field(e => e.Title).FragmentSize(100).NumberOfFragments(2)
          , f => f.Field(e => e.Author).FragmentSize(50).NumberOfFragments(2)
          , f => f.Field(e => e.Location).FragmentSize(50).NumberOfFragments(2)
          , f => f.Field(e => e.DateOfMeeting)
          , f => f.Field(e => e.UploadDate)
          , f => f.Field(e => e.CompanyName)
          , f => f.Field(e=>e.SectorName)
          , f => f.Field(e=>e.RegionName)
          //.Field(e => e.Attachment.T)
          )
          )
  );
            }

            else
            {
                var filArr = filter.Split(";".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                var filters = new List<Func<QueryContainerDescriptor<Document>, QueryContainer>>();
              
                if (filArr[0] == "SEC")
                {
                    filters.Add(f => f.Terms(t => t.Field(f1 => f1.SectorCode).Terms(filArr[1])));
                }
                if (filArr[0] == "COU")
                {
                    filters.Add(f => f.Terms(t => t.Field(f1 => f1.RegionCode).Terms(filArr[1])));
                }
                searchResults = this.ElasticClient.Search<Document>(s => s.From(startIndex).Size(10).Query(q => q.Bool(b => b
.Must(sh => sh
 .MultiMatch(mp => mp
 .Query(query).MinimumShouldMatch(MinimumShouldMatch.Percentage(50))
 .Fields(f => f.Field(f1 => f1.Attachment.Content).Field(f1 => f1.Author).Field(f1 => f1.Title).Field(f1 => f1.Location).Field(f1 => f1.CompanyName).Field(f1 => f1.SectorName).Field(f1 => f1.RegionName).Field(f1 => f1.Country))))
.Should(sh => sh
 .MultiMatch(mp => mp
 .Query(query).Type(TextQueryType.Phrase).Slop(50).MinimumShouldMatch(MinimumShouldMatch.Percentage(75))
 .Fields(f => f.Field(f1 => f1.Attachment.Content).Field(f1 => f1.Author).Field(f1 => f1.Title).Field(f1 => f1.Location).Field(f1 => f1.CompanyName).Field(f1 => f1.SectorName).Field(f1 => f1.RegionName).Field(f1 => f1.Country))))
 .Filter(filters)
 ))
 //.Query(q => q.Bool(b => b
 //.Should(sh => sh
 //.MultiMatch(mp => mp
 //.Query(query)
 //.Fields(f => f.Field(f1 => f1.Attachment.Content).Field(f1 => f1.Author).Field(f1 => f1.Title).Field(f1 => f1.Location).Field(f1 => f1.CompanyName))))))
 .Sort(so => so.Field(fi => SortFromUserInput(fi, sortField))) //.Scroll("1m")
                                                               //.Fields(f => f.Fields(f1 => f1.Attachment.Content, f2 => f2.Title, f4=>f4.Author, f5=>f5.CompanyName, f6=>f6.DateOfMeeting, f7=>f7.UploadDate)))).Sort(so=>so.Field(fi=>SortFromUserInput(fi,""))) //.Scroll("1m")
 .Highlight(h => h.PreTags("<b style='color:orange'>")
         .PostTags("</b>")
         .Fields
         (
          //f=>f.Field("*").FragmentSize(150).NumberOfFragments(3)
          f => f.Field(e => e.Attachment.Content).FragmentSize(150).NumberOfFragments(3).NoMatchSize(50)
         , f => f.Field(e => e.Title).FragmentSize(100).NumberOfFragments(2)
         , f => f.Field(e => e.Author).FragmentSize(50).NumberOfFragments(2)
         , f => f.Field(e => e.Location).FragmentSize(50).NumberOfFragments(2)
         , f => f.Field(e => e.DateOfMeeting)
         , f => f.Field(e => e.UploadDate)
         , f => f.Field(e => e.CompanyName)
         //.Field(e => e.Attachment.T)
         )
         )
 );
            }


            //        this.ElasticClient.Search<Document>(s => s
            //.Query(q => q
            //    .Match(c => c
            //        .Field(p => p.ABC)
            //        .Query(keyz)
            //    )
            //    || q.Match(c => c
            //        .Field(p => p.XYZ)
            //        .Query(keyz)
            //    )
            //)
            //);
            //var highlights = searchResults.Hits.Select(h => h.Highlights.Values.Select(v => string.Join(",", v.Highlights)));
            //foreach (var highlight in highlights)
            //{
            //    Console.WriteLine(highlight.FirstOrDefault());
            //}
            //return highlights;
            //var results = this.ElasticClient.Scroll<Document>("2m", searchResults.ScrollId);
            //while (results.Documents.Any())
            //{
            //    foreach (var doc in results.Fields)
            //    {
            //        //indexedList.Add(doc.Value<string>("propertyName"));
            //    }
            //    searchResults.Documents.ToList().AddRange(results.Documents.ToList());
            //    results = this.ElasticClient.Scroll<Document>("2m", results.ScrollId);
            //}
            return (ISearchResponse<T>)searchResults;
        }

        private IFieldSort SortFromUserInput(SortFieldDescriptor<Document> f, string userInput)
        {
            f.Order(Nest.SortOrder.Descending);

            switch (userInput)
            {
                case "uploadDate":
                    f.Field(ff => ff.UploadDate);
                    break;
                case "meetDate":
                    f.Field(ff => ff.DateOfMeeting);
                    break;
                default:
                    f.Field("_score");
                    f.Descending();
                    break;
            }

            return f;
        }

        public bool InsertESGCommentNextStepElastic( ESGSearch esgSearch )
        {
             var response= this.ElasticClient.Update(new DocumentPath<ESGSearch>(esgSearch), u => u.DocAsUpsert(true).Doc(esgSearch));
            return response.IsValid;
        }
    }
}
