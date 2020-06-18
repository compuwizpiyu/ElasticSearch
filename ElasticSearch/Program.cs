using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ElasticSearch.ElasticEntities;
using EMDBNetData.DataLayer;
using Nest;
namespace ElasticSearch
{
    class Program
    {
        private static string CallReportPath = @"D:\CallReport\";
        static void Main(string[] args)
        {
            //CreateIndex();
            //CreateIndexESG();
            //var node = new Uri("http://SPRWKS118672:9200");
            CreateLearingCenterIndex();
            Console.ReadLine();
        }

        /// <summary>
        /// Create index for Call report documents
        /// </summary>
        /// <param name="documentsIndex"></param>
        public static void CreateIndex(IndexName documentsIndex = null)
        {
            var test = "docstest";
            var index = test;
            var node = new Uri("http://rcovlnx3532.corp.frk.com:9200/");

            var connectionSettings = new ConnectionSettings(node)
              .DefaultMappingFor<Document>(m => m.IndexName(index)
              );
            var client = new ElasticClient(connectionSettings);
            try
            {
                var indexResponse = client.CreateIndex(index, c => c
                                         .Settings(s => s.Setting("index.soft_deletes.enabled", true)
                                        .Analysis(a => a
                      .Analyzers(ad => ad
                        .Custom("windows_path_hierarchy_analyzer", ca => ca
                          .Tokenizer("windows_path_hierarchy_tokenizer")
                        )
                        .Pattern("my_pattern_analyzer", p => p
                        .Pattern("\\W|_")
                        .Lowercase()
                            )
                        .Custom("auto_complete", au => au.Tokenizer("standard").Filters("lowercase", "asciifolding", "auto-complete-filter"))
                        .Custom("auto-complete-id", au => au.Tokenizer("standard").Filters("lowercase", "asciifolding", "auto-complete-id-filter"))

                      )
                       .TokenFilters(tokenFilter => tokenFilter
                                                               .EdgeNGram("auto-complete-filter", (t) => t.MinGram(3).MaxGram(20))
                                                               .EdgeNGram("auto-complete-id-filter", t => t.MinGram(1).MaxGram(5)))
                      .Tokenizers(t => t
                        .PathHierarchy("windows_path_hierarchy_tokenizer", ph => ph
                          .Delimiter('\\')
                               )
                        .NGram("nGramTokenizer", tokenizer => tokenizer.MinGram(3).MaxGram(20).TokenChars(TokenChar.Letter, TokenChar.Digit, TokenChar.Punctuation, TokenChar.Symbol))
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
                        .Text(s => s.Name(n => n.Title).Analyzer("my_pattern_analyzer")) //need to add this to break Title search with "_"
                        .Text(s => s.Name(n => n.CompanyName).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
                        .Text(s => s.Name(n => n.Author).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
                        .Text(s => s.Name(n => n.Country).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
                        .Text(s => s.Name(n => n.Location).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
                        .Text(s => s.Name(n => n.RegionName).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
                        .Text(s => s.Name(n => n.SectorName).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
                        //.Text(s => s.Name(n => n.Attachment.Content).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
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
                PopulateIndex(client);
            }
            catch (Exception ex) { }

        }

        /// <summary>
        /// Populate the Index with call report documents
        /// </summary>
        /// <param name="client"></param>
        private static void PopulateIndex(ElasticClient client)
        {
            var directory = @"\\stovsvr3484\CallReport\";//System.Configuration.ConfigurationManager.AppSettings["CallReportPath"].ToString();
            var callReportsCollection = Directory.GetFiles(directory, "*.doc"); //this will fetch both doc and docx
            //callReportsCollection.ToList().AddRange(Directory.GetFiles(directory, "*.doc"));
            ConcurrentBag<string> reportsBag = new ConcurrentBag<string>(callReportsCollection);
            int i = 0;
            var callReportElasticDataSet = new DLCallReportSearch().GetCallReportDetailsForElastic();//.AsEnumerable();//.Take(50).CopyToDataTable();
            try
            {
                Parallel.ForEach(reportsBag, callReport =>
                //Array.ForEach(callReportsCollection,callReport=>
                {
                    var base64File = Convert.ToBase64String(File.ReadAllBytes(callReport));
                    var fileSavedName = callReport.Replace(directory, "");
                    // var dt = dLCallReportSearch.GetCallFileName(fileSavedName.Replace("'", "''"));//replace the ' in a file name with '';
                    var rows = callReportElasticDataSet.Select("CALL_SAVE_FILE like '%" + fileSavedName.Replace("'", "''") + "'");
                    if (rows != null && rows.Count() > 0)
                    {
                        var row = rows.FirstOrDefault();
                        //foreach (DataRow row in rows)
                        //{
                        i++;
                        client.Index(new Document
                        {
                            Id = i,
                            DocId = Convert.ToInt32(row["CALL_ID"].ToString()),
                            Path = row["CALL_SAVE_FILE"].ToString().Replace(CallReportPath, ""),
                            Title = row["CALL_FILE"].ToString().Replace(CallReportPath, ""),
                            Author = row["USER_NAME"].ToString(),
                            DateOfMeeting = string.IsNullOrEmpty(row["CALL_DT"].ToString()) ? (DateTime?)null : Convert.ToDateTime(row["CALL_DT"].ToString()),
                            Location = row["CALL_LOCATION"].ToString(),
                            UploadDate = string.IsNullOrEmpty(row["CALL_REPORT_DT"].ToString()) ? (DateTime?)null : Convert.ToDateTime(row["CALL_REPORT_DT"].ToString()),
                            CompanyName = row["COMP_NAME"].ToString(),
                            CompanyId = Convert.ToInt32(row["COMP_ID"].ToString()),
                            Country = row["COU_NAME"].ToString(),
                            CountryCode = row["COU_CD"].ToString(),
                            RegionCode = row["REGION_CODE"].ToString(),
                            RegionName = row["REGION_NAME"].ToString(),
                            SectorCode = row["SECTOR_CD"].ToString(),
                            SectorName = row["SECTOR_NAME"].ToString(),
                            Content = base64File
                        }, p => p.Pipeline("attachments"));
                        //}
                    }
                });
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Create Index for ESG Comments and NextStep search
        /// </summary>
        /// <param name="documentsIndex"></param>
        public static void CreateIndexESG(IndexName documentsIndex = null)
        {
            var test = "esgsearch";
            var index = test;
            var node = new Uri("http://HKGWKS139406:9200");

            var connectionSettings = new ConnectionSettings(node)
              .DefaultMappingFor<ESGSearch>(m => m.IndexName(index)
              );
            var client = new ElasticClient(connectionSettings);
            try
            {
                var indexResponse = client.CreateIndex(index, c => c
                                         .Settings(s => s.Setting("index.soft_deletes.enabled", true)
                                        .Analysis(a => a
                      .Analyzers(ad => ad
                       
                        .Custom("auto_complete", au => au.Tokenizer("standard").Filters("lowercase", "asciifolding", "auto-complete-filter"))
                        .Custom("auto-complete-id", au => au.Tokenizer("standard").Filters("lowercase", "asciifolding", "auto-complete-id-filter"))

                      )
                       .TokenFilters(tokenFilter => tokenFilter
                                                               .EdgeNGram("auto-complete-filter", (t) => t.MinGram(3).MaxGram(20))
                                                               .EdgeNGram("auto-complete-id-filter", t => t.MinGram(1).MaxGram(5)))
                     
                      )
                  )
                  .Mappings(m => m
                    .Map<ESGSearch>(mp => mp
                      .AutoMap()
                      .AllField(all => all
                        .Enabled(false)
                      )
                      .Properties(ps => ps
                        .Text(s => s.Name(n => n.ESGComments).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
                        .Text(s => s.Name(n => n.ESGNextSteps).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
                      )
                    )
                  )
                );
                
                PopulateESGIndex(client);
            }
            catch (Exception ex) { }

        }

        private static void PopulateESGIndex(ElasticClient client)
        {
            var dt = new DLEsgEngagementDetails().GetAllEngagements();
            foreach(DataRow dr in dt.Rows)
            {
                client.Index(new ESGSearch
                {
                    ESGId=Convert.ToInt32(dr["ESG_Engagement_ID"]),
                    ESGComments=dr["REMARKS"].ToString(),
                    ESGNextSteps=dr["Next_Step"].ToString()
                },p=>p.Index("esgsearch"));
            }
        }

        public static void CreateLearingCenterIndex(IndexName doc=null)
        {
            var test = "learningcenter";
            var index = test;
            var node = new Uri("http://HKGWKS139406:9200");

            var connectionSettings = new ConnectionSettings(node)
              .DefaultMappingFor<LearningCenter>(m => m.IndexName(index)
              );
            var client = new ElasticClient(connectionSettings);
            try
            {
                var indexResponse = client.CreateIndex(index, c => c
                                         .Settings(s => s.Setting("index.soft_deletes.enabled", true)
                                        .Analysis(a => a
                      .Analyzers(ad => ad
                        .Custom("windows_path_hierarchy_analyzer", ca => ca
                          .Tokenizer("windows_path_hierarchy_tokenizer")
                        )
                        .Pattern("my_pattern_analyzer", p => p
                        .Pattern("\\W|_")
                        .Lowercase()
                            )
                        .Custom("auto_complete", au => au.Tokenizer("standard").Filters("lowercase", "asciifolding", "auto-complete-filter"))
                        .Custom("auto-complete-id", au => au.Tokenizer("standard").Filters("lowercase", "asciifolding", "auto-complete-id-filter"))

                      )
                       .TokenFilters(tokenFilter => tokenFilter
                                                               .EdgeNGram("auto-complete-filter", (t) => t.MinGram(3).MaxGram(20))
                                                               .EdgeNGram("auto-complete-id-filter", t => t.MinGram(1).MaxGram(5)))
                      .Tokenizers(t => t
                        .PathHierarchy("windows_path_hierarchy_tokenizer", ph => ph
                          .Delimiter('\\')
                               )
                        .NGram("nGramTokenizer", tokenizer => tokenizer.MinGram(3).MaxGram(20).TokenChars(TokenChar.Letter, TokenChar.Digit, TokenChar.Punctuation, TokenChar.Symbol))
                        )
                      )
                  )
                  .Mappings(m => m
                    .Map<LearningCenter>(mp => mp
                      .AutoMap()
                      .AllField(all => all
                        .Enabled(false)
                      )
                      .Properties(ps => ps
                        .Text(s => s
                          .Name(n => n.Path)
                          .Analyzer("windows_path_hierarchy_analyzer")
                        )
                        .Text(s => s.Name(n => n.Title).Analyzer("my_pattern_analyzer")) //need to add this to break Title search with "_"
                        .Text(s => s.Name(n => n.Headline).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
                        .Text(s => s.Name(n => n.Author).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
                        .Text(s => s.Name(n => n.DocumentType).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
                        .Text(s => s.Name(n => n.SectorName).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
                        .Text(s => s.Name(n => n.Attachment.Content).Analyzer("auto_complete").Fields(ff => ff.Keyword(k => k.Name("keyword"))))
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
                        .Attachment<LearningCenter>(a => a
                          .Field(f => f.Content)
                          .TargetField(f => f.Attachment)
                        )
                        .Remove<LearningCenter>(r => r
                          .Field(f => f.Content)
                        )
                      )
                    );
                PopulateLearningCenterIndex(client);
            }
            catch (Exception ex) { }

        }

        private static void PopulateLearningCenterIndex(ElasticClient client)
        {
            var directory = @"\\stovsvr3484\LearningCenterUploadRpt";//System.Configuration.ConfigurationManager.AppSettings["CallReportPath"].ToString();
            var callReportsCollection = Directory.GetFiles(directory); //this will fetch all files
            //callReportsCollection.ToList().AddRange(Directory.GetFiles(directory, "*.doc"));
            ConcurrentBag<string> reportsBag = new ConcurrentBag<string>(callReportsCollection);
            int i = 0;
            var learningDocElastic = new DLEsgEngagementDetails().GetLearingDocs();//.AsEnumerable();//.Take(50).CopyToDataTable();
            try
            {
                //Parallel.ForEach(reportsBag, callReport =>
                Array.ForEach(callReportsCollection,callReport=>
                {
                    var base64File = Convert.ToBase64String(File.ReadAllBytes(callReport));
                    var fileSavedName = callReport.Replace(directory, "").Replace(@"\","");
                    // var dt = dLCallReportSearch.GetCallFileName(fileSavedName.Replace("'", "''"));//replace the ' in a file name with '';
                    var rows = learningDocElastic.Select("FILENAME like '%" + fileSavedName.Replace("'", "''") + "'");
                    //var rows = from l in learningDocElastic.AsEnumerable() where l.Field<string>("FileName").Equals(fileSavedName, StringComparison.OrdinalIgnoreCase) select l;
                    if (rows != null && rows.Count() > 0)
                    {
                        var row = rows.FirstOrDefault();
                        //foreach (DataRow row in rows)
                        //{
                        i++;
                        client.Index(new LearningCenter
                        {
                            Id = i,
                            LearningDetailsId = Convert.ToInt32(row["ID"].ToString()),
                            Path = row["FilePath"].ToString(),
                            Title = row["FileName"].ToString(),
                            Author = row["Author"].ToString(),                           
                            UploadDate = string.IsNullOrEmpty(row["UploadDate"].ToString()) ? (DateTime?)null : Convert.ToDateTime(row["UploadDate"].ToString()),
                            Headline = row["Headline"].ToString(),
                            DocumentType = row["DocType"].ToString(),                            
                            SectorCode = row["SectorCode"].ToString(),
                            SectorName = row["SectorName"].ToString(),
                            Content = base64File
                        }, p => p.Pipeline("attachments"));
                        //}
                    }
                });
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }

}

