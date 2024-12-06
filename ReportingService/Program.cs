using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using static ReportingService.GeneralSetup;

namespace ReportingService
{
    class Program
    {
        static void Main(string[] args)
        {
            PerformTask().Wait();
        }

        static async Task PerformTask()
        {
            // C&T Extracts
            bool cnt = false; //await CTExtracts.PatientExtract();
            if (cnt)
            {
                await CTExtracts.PatientStatus();
                await CTExtracts.PatientArt();
                await CTExtracts.IPTExtract();
                await CTExtracts.DefaulterTracing();
                await CTExtracts.CovidExtract();
                await CTExtracts.PatientLabs();
                await CTExtracts.PatientPharmacy();
                await CTExtracts.RelationshipsExtract();
                await CTExtracts.PatientVisits();
                await CTExtracts.PatientBaselines();
                await CTExtracts.ArtFastTrack();
                await CTExtracts.EACExtract();
                await CTExtracts.GBVExtract();
                await CTExtracts.AdverseEvents();
                await CTExtracts.DrugAlcoholExtract();
                await CTExtracts.DepressionScreening();
                await CTExtracts.OtzExtract();
                await CTExtracts.OvcExtract();
                await CTExtracts.ContactListing();
                await CTExtracts.AllergiesChronicIllness();
                await CTExtracts.IITRiskScores();
                await ServicesDTOs.BuildMetricsAsync();

                Console.WriteLine("All C&T extracts processed.");
            }
            else
            {
                Console.WriteLine("C&T patient processing failed.");
            }

            // PrEP Extracts
            bool prep = false; //await PrepExtracts.PatientPrepExtract();
            if (prep)
            {
                await PrepExtracts.PrepMonthlyRefillExtract();
                await PrepExtracts.PrepCareTerminationExtract();
                await PrepExtracts.PrepVisitExtract();
                await PrepExtracts.PrepLabExtract();
                await PrepExtracts.PrepPharmacyExtract();
                await PrepExtracts.PrepBehaviourRiskExtract();
                await PrepExtracts.PrepAdverseEventExtract();

                Console.WriteLine("All PrEP extracts processed.");
            }
            else
            {
                Console.WriteLine("PrEP patient processing failed.");
            }

            // MNCH Extracts
            bool mnch = false; //await MnchExtracts.PatientMnchExtract();
            if (mnch)
            {
                await MnchExtracts.MnchEnrolmentExtract();
                await MnchExtracts.HeiExtract();
                await MnchExtracts.AncVisitExtract();
                await MnchExtracts.CwcVisitExtract();
                await MnchExtracts.MnchLabExtract();
                await MnchExtracts.MnchImmunizationExtract();
                await MnchExtracts.PncVisitExtract();
                await MnchExtracts.CwcEnrolmentExtract();
                await MnchExtracts.MotherBabyPairExtract();
                await MnchExtracts.MatVisitExtract();
                await MnchExtracts.MnchArtExtract();

                Console.WriteLine("All MNCH extracts processed.");
            }
            else
            {
                Console.WriteLine("MNCH patient processing failed.");
            }
        }
    }
}
