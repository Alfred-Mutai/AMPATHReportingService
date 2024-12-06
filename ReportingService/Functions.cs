using MySql.Data.MySqlClient;
using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static ReportingService.GeneralSetup;

namespace ReportingService
{
    class CTExtracts
    {
        public static async Task<bool> PatientExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = true,
                tables = new paramTables()
                {
                    queueTable = "ndwr_all_patients_build_queue",
                    extractTable = "ndwr_all_patients_extract",
                    procedureName = "build_NDWR_all_patients_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = "(person_id) SELECT DISTINCT person_id FROM etl.flat_hiv_summary_v15b WHERE is_clinical_encounter = 1 AND next_clinical_datetime_hiv IS NULL;"
                }
            };

            Console.WriteLine($"1. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PatientStatus()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = true,
                tables = new paramTables()
                {
                    queueTable = "ndwr_all_patient_status_extract_build_queue",
                    extractTable = "ndwr_all_patient_status_extract",
                    procedureName = "build_NDWR_all_patient_status_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct PatientPK from {Database}.ndwr_all_patients_extract where StatusAtCCC in('dead','ltfu','transfer_out'));"
                }
            };

            Console.WriteLine($"2. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PatientArt()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = true,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_art_extract_build_queue",
                    extractTable = "ndwr_patient_art_extract",
                    procedureName = "build_NDWR_patient_art_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct PatientPK from {Database}.ndwr_all_patients_extract);"
                }
            };

            Console.WriteLine($"3. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> IPTExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_ipt_extract_build_queue",
                    procedureName = "build_ndwr_patient_ipt_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = "(select distinct person_id from etl.flat_hiv_summary_v15b where encounter_type in (1,2));"
                }
            };

            Console.WriteLine($"4. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> DefaulterTracing()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                tables = new paramTables()
                {
                    queueTable = "ndwr_defaulter_tracing_extract_build_queue",
                    procedureName = "build_NDWR_defaulter_tracing_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct patient_id from amrs.encounter e where encounter_type in (21) and date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and date_created < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}' and voided = 0);"
                }
            };

            Console.WriteLine($"5. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> CovidExtract()
        {
            // added limit 0 for it not to run
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_covid_extract_build_queue",
                    procedureName = "build_NDWR_covid_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct patient_id from amrs.encounter where encounter_type in (208) and date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and date_created < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}' and voided = 0 limit 0);"
                }
            };

            Console.WriteLine($"6. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PatientLabs()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 50,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_labs_extract_build_queue",
                    procedureName = "build_NDWR_ndwr_patient_labs_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $@"(select distinct person_id from amrs.obs where concept_id in (856,730,5497) and voided = 0 and date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}');"
                }
            };

            Console.WriteLine($"7. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PatientPharmacy()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 50,
                tables = new paramTables()
                {
                    queueTable = "ndwr_pharmacy_build_queue",
                    procedureName = "build_NDWR_pharmacy"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id from etl.flat_hiv_summary_v15b where cur_arv_meds is not null and date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate} ' and date_created < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}');"
                }
            };

            Console.WriteLine($"8. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> RelationshipsExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = false,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_ct_relationships_extract_build_queue",
                    extractTable = "ndwr_patient_ct_relationships_extract",
                    procedureName = "build_NDWR_ct_relationships_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct PatientPK from {Database}.ndwr_all_patients_extract);"
                }
            };

            Console.WriteLine($"9. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PatientVisits()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 20,
                tables = new paramTables()
                {
                    queueTable = "ndwr_all_patient_visits_extract_build_queue",
                    procedureName = "build_NDWR_all_patient_visits_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(person_id) SELECT DISTINCT person_id FROM etl.flat_hiv_summary_v15b WHERE date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' AND date_created< '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}';"
                }
            };

            Console.WriteLine($"10. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PatientBaselines()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = false,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_baselines_extract_build_queue",
                    extractTable = "ndwr_patient_baselines_extract",
                    procedureName = "build_ndwr_patient_baselines_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct PatientPK from {Database}.ndwr_all_patients_extract);"
                }
            };

            Console.WriteLine($"11. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> ArtFastTrack()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 20,
                tables = new paramTables()
                {
                    queueTable = "ct_art_fast_track_build_queue",
                    procedureName = "build_ct_art_fast_track"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id from etl.flat_hiv_summary_v15b where encounter_type in (186) and encounter_datetime >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and encounter_datetime < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}');"
                }
            };

            Console.WriteLine($"12. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> EACExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_ct_eac_build_queue",
                    procedureName = "build_ndwr_ct_eac"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id from amrs.obs o where o.concept_id in (10530, 10522, 10526) and o.obs_datetime >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and o.obs_datetime < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}');"
                }
            };

            Console.WriteLine($"13. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> GBVExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_gender_based_violence_screening_extract_build_queue",
                    procedureName = "build_NDWR_gender_based_violence_screening_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id from amrs.obs o where (concept_id = 11866 and value_coded = 1065) or (concept_id = 11865 and value_coded = 1065) or (concept_id = 9303 and value_coded = 1065)or (concept_id = 12054 and value_coded = 1065) and voided = 0 and obs_datetime >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and obs_datetime < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}');"
                }
            };

            Console.WriteLine($"14. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> AdverseEvents()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_adverse_events_build_queue",
                    procedureName = "build_ndwr_adverse_events"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select DISTINCT person_id from amrs.obs o where (concept_id = 6967 and value_coded = 3) or (concept_id = 6967 and value_coded = 512) or (concept_id = 6967 and value_coded = 5978) or (concept_id = 6967 and value_coded = 8295) or (concept_id = 6967 and value_coded = 877) or (concept_id = 6967 and value_coded = 1443) or (concept_id = 6967 and value_coded = 5949) or (concept_id = 6967 and value_coded = 6636) and obs_datetime >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and obs_datetime < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}');"
                }
            };

            Console.WriteLine($"15. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> DrugAlcoholExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_all_drugalcoholscreenings_build_queue",
                    procedureName = "build_NDWR_drugalcoholscreenings_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id as person_id from amrs.obs o where o.concept_id in (1684) and o.obs_datetime >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and o.obs_datetime < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}')"
                }
            };

            Console.WriteLine($"16. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> DepressionScreening()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_depression_screening_extract_build_queue",
                    procedureName = "build_NDWR_depression_screening_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id as person_id from amrs.obs o where o.concept_id in (7815) and o.obs_datetime >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and o.obs_datetime < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}')"
                }
            };

            Console.WriteLine($"17. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> OtzExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_otz_extract_build_queue",
                    procedureName = "build_NDWR_otz_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id from etl.flat_obs where encounter_type = 284 and encounter_datetime >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and encounter_datetime < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}')"
                }
            };

            Console.WriteLine($"18. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> OvcExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_ct_ovc_extract_build_queue",
                    procedureName = "build_NDWR_ct_ovc_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id as person_id from amrs.obs o where o.concept_id in (9807) and o.obs_datetime >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and o.obs_datetime < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}')"
                }
            };

            Console.WriteLine($"19. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> ContactListing()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_contact_listing_build_queue",
                    procedureName = "build_ndwr_contact_listing"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct patient_id from etl.flat_family_testing where date_elicited >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and date_elicited < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}')"
                }
            };

            Console.WriteLine($"20. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> AllergiesChronicIllness()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_allergies_chronic_ilnesses_extract_build_queue",
                    procedureName = "build_NDWR_allergies_chronic_ilnesses_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id as person_id from amrs.obs o where o.concept_id in (6011) and o.obs_datetime >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and o.obs_datetime < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}')"
                }
            };

            Console.WriteLine($"21. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> IITRiskScores()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 50000,
                tables = new paramTables()
                {
                    queueTable = "ndwr_iit_risk_factors_build_queue",
                    procedureName = "build_NDWR_iit_risk_factors"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select DISTINCT person_id from predictions.ml_weekly_predictions where week='{ServicesDTOs.GetMLWeek()}')"
                }
            };

            parameters.sqlParameters = new MySqlParameter[]
            {
                new MySqlParameter("@param1", parameters.queryType),
                new MySqlParameter("@param2", ServicesDTOs.GetDateRange(DateRangeType.MonthReportDates).startDate),
                new MySqlParameter("@param3", ServicesDTOs.GetDateRange(DateRangeType.MonthReportDates).endDate),
                new MySqlParameter("@param4", 1),
                new MySqlParameter("@param5", parameters.QuerySize),
                new MySqlParameter("@param6", 1),
                new MySqlParameter("@param7", true)
            };

            Console.WriteLine($"22. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> CancerScreening()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_cervical_cancer_extract_build_queue",
                    procedureName = "build_NDWR_cervical_cancer_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id from etl.flat_cervical_cancer_screening where encounter_datetime >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and encounter_datetime < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}')"
                }
            };

            Console.WriteLine($"23. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
    }
    class PrepExtracts
    {
        public static async Task<bool> PatientPrepExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = true,
                tables = new paramTables()
                {
                    queueTable = "ndwr_all_patients_build_queue",
                    extractTable = "ndwr_all_patients_extract",
                    procedureName = "build_NDWR_all_patients_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = "(person_id) SELECT DISTINCT person_id FROM etl.flat_hiv_summary_v15b WHERE is_clinical_encounter = 1 AND next_clinical_datetime_hiv IS NULL;"
                }
            };

            Console.WriteLine($"1. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PrepMonthlyRefillExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = true,
                tables = new paramTables()
                {
                    queueTable = "ndwr_all_patient_status_extract_build_queue",
                    extractTable = "ndwr_all_patient_status_extract",
                    procedureName = "build_NDWR_all_patient_status_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct PatientPK from {Database}.ndwr_all_patients_extract where StatusAtCCC in('dead','ltfu','transfer_out'));"
                }
            };

            Console.WriteLine($"2. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PrepCareTerminationExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = true,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_art_extract_build_queue",
                    extractTable = "ndwr_patient_art_extract",
                    procedureName = "build_NDWR_patient_art_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct PatientPK from {Database}.ndwr_all_patients_extract);"
                }
            };

            Console.WriteLine($"3. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PrepVisitExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_ipt_extract_build_queue",
                    procedureName = "build_ndwr_patient_ipt_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = "(select distinct person_id from etl.flat_hiv_summary_v15b where encounter_type in (1,2));"
                }
            };

            Console.WriteLine($"4. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PrepLabExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                tables = new paramTables()
                {
                    queueTable = "ndwr_defaulter_tracing_extract_build_queue",
                    procedureName = "build_NDWR_defaulter_tracing_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct patient_id from amrs.encounter e where encounter_type in (21) and date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and date_created < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}' and voided = 0);"
                }
            };

            Console.WriteLine($"5. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PrepPharmacyExtract()
        {
            // added limit 0 for it not to run
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_covid_extract_build_queue",
                    procedureName = "build_NDWR_covid_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct patient_id from amrs.encounter where encounter_type in (208) and date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and date_created < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}' and voided = 0 limit 0);"
                }
            };

            Console.WriteLine($"6. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PrepBehaviourRiskExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 50,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_labs_extract_build_queue",
                    procedureName = "build_NDWR_ndwr_patient_labs_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $@"(select distinct person_id from amrs.obs where concept_id in (856,730,5497) and voided = 0 and date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}');"
                }
            };

            Console.WriteLine($"7. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PrepAdverseEventExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 50,
                tables = new paramTables()
                {
                    queueTable = "ndwr_pharmacy_build_queue",
                    procedureName = "build_NDWR_pharmacy"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id from etl.flat_hiv_summary_v15b where cur_arv_meds is not null and date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate} ' and date_created < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}');"
                }
            };

            Console.WriteLine($"8. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
    }
    class MnchExtracts
    {
        public static async Task<bool> PatientMnchExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = true,
                tables = new paramTables()
                {
                    queueTable = "ndwr_all_patients_build_queue",
                    extractTable = "ndwr_all_patients_extract",
                    procedureName = "build_NDWR_all_patients_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = "(person_id) SELECT DISTINCT person_id FROM etl.flat_hiv_summary_v15b WHERE is_clinical_encounter = 1 AND next_clinical_datetime_hiv IS NULL;"
                }
            };

            Console.WriteLine($"1. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> MnchEnrolmentExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = true,
                tables = new paramTables()
                {
                    queueTable = "ndwr_all_patient_status_extract_build_queue",
                    extractTable = "ndwr_all_patient_status_extract",
                    procedureName = "build_NDWR_all_patient_status_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct PatientPK from {Database}.ndwr_all_patients_extract where StatusAtCCC in('dead','ltfu','transfer_out'));"
                }
            };

            Console.WriteLine($"2. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> HeiExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = true,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_art_extract_build_queue",
                    extractTable = "ndwr_patient_art_extract",
                    procedureName = "build_NDWR_patient_art_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct PatientPK from {Database}.ndwr_all_patients_extract);"
                }
            };

            Console.WriteLine($"3. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> AncVisitExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_ipt_extract_build_queue",
                    procedureName = "build_ndwr_patient_ipt_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = "(select distinct person_id from etl.flat_hiv_summary_v15b where encounter_type in (1,2));"
                }
            };

            Console.WriteLine($"4. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> CwcVisitExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                tables = new paramTables()
                {
                    queueTable = "ndwr_defaulter_tracing_extract_build_queue",
                    procedureName = "build_NDWR_defaulter_tracing_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct patient_id from amrs.encounter e where encounter_type in (21) and date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and date_created < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}' and voided = 0);"
                }
            };

            Console.WriteLine($"5. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> MnchLabExtract()
        {
            // added limit 0 for it not to run
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 100,
                tables = new paramTables()
                {
                    queueTable = "ndwr_covid_extract_build_queue",
                    procedureName = "build_NDWR_covid_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct patient_id from amrs.encounter where encounter_type in (208) and date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and date_created < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}' and voided = 0 limit 0);"
                }
            };

            Console.WriteLine($"6. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> MnchImmunizationExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 50,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_labs_extract_build_queue",
                    procedureName = "build_NDWR_ndwr_patient_labs_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $@"(select distinct person_id from amrs.obs where concept_id in (856,730,5497) and voided = 0 and date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}');"
                }
            };

            Console.WriteLine($"7. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> PncVisitExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 50,
                tables = new paramTables()
                {
                    queueTable = "ndwr_pharmacy_build_queue",
                    procedureName = "build_NDWR_pharmacy"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id from etl.flat_hiv_summary_v15b where cur_arv_meds is not null and date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate} ' and date_created < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}');"
                }
            };

            Console.WriteLine($"8. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> CwcEnrolmentExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = false,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_ct_relationships_extract_build_queue",
                    extractTable = "ndwr_patient_ct_relationships_extract",
                    procedureName = "build_NDWR_ct_relationships_extract"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct PatientPK from {Database}.ndwr_all_patients_extract);"
                }
            };

            Console.WriteLine($"9. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> MotherBabyPairExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 20,
                tables = new paramTables()
                {
                    queueTable = "ndwr_all_patient_visits_extract_build_queue",
                    procedureName = "build_NDWR_all_patient_visits_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(person_id) SELECT DISTINCT person_id FROM etl.flat_hiv_summary_v15b WHERE date_created >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' AND date_created< '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}';"
                }
            };

            Console.WriteLine($"10. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> MatVisitExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 200,
                truncateExtract = false,
                tables = new paramTables()
                {
                    queueTable = "ndwr_patient_baselines_extract_build_queue",
                    extractTable = "ndwr_patient_baselines_extract",
                    procedureName = "build_ndwr_patient_baselines_extract_v2"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct PatientPK from {Database}.ndwr_all_patients_extract);"
                }
            };

            Console.WriteLine($"11. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
        public static async Task<bool> MnchArtExtract()
        {
            Params parameters = new Params()
            {
                start = true,
                QuerySize = 20,
                tables = new paramTables()
                {
                    queueTable = "ct_art_fast_track_build_queue",
                    procedureName = "build_ct_art_fast_track"
                },
                queries = new paramQueries()
                {
                    buildQueryAddOn = $"(select distinct person_id from etl.flat_hiv_summary_v15b where encounter_type in (186) and encounter_datetime >= '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).startDate}' and encounter_datetime < '{ServicesDTOs.GetDateRange(DateRangeType.ReportDates).endDate}');"
                }
            };

            Console.WriteLine($"12. {parameters.tables.procedureName}");

            return await ServicesDTOs.BuildExtractAsync(parameters);
        }
    }
}
