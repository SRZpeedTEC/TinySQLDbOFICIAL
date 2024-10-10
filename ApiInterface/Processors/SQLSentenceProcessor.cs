using ApiInterface.InternalModels;
using ApiInterface.Models;
using Entities;
using QueryProcessor;
using System.Diagnostics;

namespace ApiInterface.Processors
{
    internal class SQLSentenceProcessor(Request request) : IProcessor 
    {
        public Request Request { get; } = request;

        public Response Process()
        {
            
            var sentence = this.Request.RequestBody;
            Stopwatch stopwatch = Stopwatch.StartNew();
            var result = SQLQueryProcessor.Execute(sentence, out object? data);
            stopwatch.Stop();

            long elapsedTicks = stopwatch.ElapsedTicks;
            double elapsedNanoseconds = (elapsedTicks * (1_000_000_000.0 / Stopwatch.Frequency));
            Console.WriteLine($"Tiempo transcurrido: {elapsedNanoseconds} nanosegundos");
            var response = this.ConvertToResponse(result, data);
            return response;
        }

        private Response ConvertToResponse(OperationResult result, object? data)
        {
            return new Response
            {
                Status = result.Status,
                Request = this.Request,
                ResponseBody = result.Message, // Aqui se envia la informacion de la respuesta
                ResponseData = data

            };
        }
    }
}
