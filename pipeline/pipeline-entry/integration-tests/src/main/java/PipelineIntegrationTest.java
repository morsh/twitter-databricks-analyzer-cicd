import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import socialposts.pipeline.sources.SocialMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class PipelineIntegrationTest {


  private void test() throws IOException, InterruptedException {
    //Run pipeline
    runTestPipeline();
    //Wait for pipeline to finish by sleeping or by listening to the output eventhubs
    Thread.sleep(1000*60*10);
    //Grab results from eventhubs and compare to expected
    evalutePipelineResult();


  }

  private void runTestPipeline(){
    System.out.println("Not implemented yet");
  }

  private void evalutePipelineResult(){
    System.out.println("Not implemented yet");
  }


  public static void main(String[] args) throws IOException, InterruptedException {
    PipelineIntegrationTest test = new PipelineIntegrationTest();
    test.test();
  }

}
