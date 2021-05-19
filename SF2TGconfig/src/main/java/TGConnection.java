import com.google.gson.*;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ByteArrayEntity;

import java.io.*;
import java.util.*;

public class TGConnection{
    static String cookie; // TG GST session cookieid

    public static HashMap<String, Set<String>> getTGInfo(connectConfigs config) throws IOException {
        cookie = getCookie(config);

        return getloadingJobs(cookie, config);
    }

    public static boolean testToken (connectConfigs config) {
        Gson gson = new Gson();
        String res;

        Request request = Request.Get("http://" + config.getTgIP() + ":9000/echo");
        request.setHeader("Authorization", "Bearer " + config.getToken());
        JsonElement jelem = null;
        try {
            jelem = gson.fromJson(request.execute().returnContent().toString(), JsonElement.class);
            res = jelem.getAsJsonObject().get("error").getAsString();
        } catch (HttpResponseException e) {
            return false;
        } catch (IOException e) {
            return false;
        }

        if (res.equalsIgnoreCase("false")) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean testLogin (connectConfigs config) throws UnsupportedEncodingException {
        Gson gson = new Gson();
        String res;
        String encoding = Base64.getEncoder().encodeToString((config.getUsername()+ ":"+ config.getPassword()).getBytes("UTF-8"));

        Request request = Request.Get("http://" + config.getTgIP() + ":14240/gsqlserver/gsql/schema?graph=" + config.getGraph());
        request.setHeader("Authorization", "Basic " + encoding);
        JsonElement jelem = null;
        try {
            jelem = gson.fromJson(request.execute().returnContent().toString(), JsonElement.class);
            res = jelem.getAsJsonObject().get("error").getAsString();
        } catch (HttpResponseException e) {
            return false;
        } catch (IOException e) {
            return false;
        }

        if (res.equalsIgnoreCase("false")) {
            return true;
        } else {
            return false;
        }
    }


    public static HashMap<String, Set<String>> getloadingJobs(String cookie, connectConfigs config) throws IOException {
        // job name -> [filenames]
        HashMap<String,Set<String>> jobs = new HashMap<>();
        Gson gson = new Gson();

        Request request = Request.Get("http://" + config.getTgIP() + ":14240/api/loading-jobs/SimSwapPoC/meta");
        request.setHeader("cookie", cookie);

        // convert response string -> json object -> json array
        JsonElement jelem = gson.fromJson(request.execute().returnContent().toString(), JsonElement.class);
        JsonArray jsonArray = jelem.getAsJsonObject().getAsJsonArray("results");

        for (JsonElement elem : jsonArray) {
            // store jobs -> [filename1,filename2,...filenameN]
            jobs.put(elem.getAsJsonObject().get("JobName").getAsString(),elem.getAsJsonObject().getAsJsonObject("FileNames").keySet());
        }
        return jobs;
    }

    public static String getCookie(connectConfigs config) throws IOException {
        String temp = null;

        // REPLACE USERNAME AND PASSWORD WITH USER INPUTS
        String tgUserCred = "{\"username\":\"tigergraph\",\"password\":\"tigergraph\"}";
        HttpEntity entity = new ByteArrayEntity(tgUserCred.getBytes("UTF-8"));

        Request request = Request.Post("http://" + config.getTgIP() + ":14240/api/auth/login");
        //HttpResponse httpResponse = request.execute().returnResponse();
        //System.out.println(httpResponse.getStatusLine());

        request.setHeader("Connection", "keep-alive");
        request.setHeader("Sec-Ch-Ua", "\"Not A; Brand \";v=\" 99 \", \" Chromium \";v=\" 90 \", \" Google Chrome \";v=\" 90 \"");
        request.setHeader("Accept", "application/json, text/plain, */*");
        request.setHeader("Sec-Ch-Ua-Mobile", "?0");
        request.setHeader("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36");
        request.setHeader("Content-Type", "application/json");
        request.setHeader("Origin", "http://127.0.0.1:14240");
        request.setHeader("Sec-Fetch-Site", "same-origin");
        request.setHeader("Sec-Fetch-Mode", "cors");
        request.setHeader("Sec-Fetch-Dest", "empty");
        request.setHeader("Referer", "http://127.0.0.1:14240/");
        request.setHeader("Accept-Language", "en-US,en;q=0.9,ar;q=0.8");
        request.body(entity);

        HttpResponse httpResponse = request.execute().returnResponse();
        //System.out.println(httpResponse.getStatusLine()); // prints http response code

        if (httpResponse.getEntity() != null) {
            temp = httpResponse.getFirstHeader("Set-Cookie").getValue();
        }

        String[] cook1 = temp.split(";",2);
        temp = cook1[0];

        return temp;
    }

}
