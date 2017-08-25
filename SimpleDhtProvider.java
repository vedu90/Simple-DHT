package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;

import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;

import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDhtProvider extends ContentProvider {

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final int SERVER_PORT = 10000;
    boolean successorFound = false;
    static String myPort;
    ArrayList<portNodeMap> sortedNodes = new ArrayList<portNodeMap>(5);
    boolean firstJoinRequest = true;
    boolean queryResultReceived = false;
    boolean deleteResultReceived = false;
    boolean[] JoinRequestReceived = new boolean[4];
    ArrayList<String> queryResults = new ArrayList<String>(8);
    String queryKey;
    String availableNodes="";
    public class sortedNodesComparator implements Comparator<portNodeMap> {
        public int compare(portNodeMap m1, portNodeMap m2) {
            return new String(m1.node).compareTo(new String(m2.node));
        }}
    class portNodeMap{

        String node;
        int port;

        void Set(String n, int p)
        {
            node = n;
            port = p;
        }

    }
    static String selfNode;
    portNodeMap predNode = new portNodeMap();
    portNodeMap succNode = new portNodeMap();

    Map<String,String> localDHT = new HashMap<String, String>();

    boolean joinRequest = false;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        try
        {
            Log.v("SimpleDHT", "Delete called "+selection);

            if(!(selection.equals("@"))&&!(selection.equals("*")))
            {
                if(DeleteFile(selection))
                {
                    Log.v("SimpleDHT", "Deleted the file locally "+selection);
                }
                else
                {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"DK",myPort,selection);
                    deleteResultReceived = false;
                }
                return 0;
            }

            DeleteLocalFiles();

            if(successorFound == true && selection.equals("*"))
            {
                Log.v("SimpleDHT", "Delete * called");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"D",myPort);
                deleteResultReceived = false;
            }
        }
        catch (Exception e)
        {
            Log.v("SimpleDHT", "Exception caught in Delete "+e);
        }


        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try
        {

            String key = (String) values.get(KEY_FIELD);
            String val = (String) values.get(VALUE_FIELD);

            Log.v("SimpleDHT", "Insert called with key "+key+" val "+val);

            insertMethod(false,key,val);
            return uri;
        }
        catch (Exception e)
        {
            Log.v("SimpleDHT", "Insert failed "+e);
        }

        return null;
    }

    public void insertMethod(boolean doLocalInsert,String key,String val)
    {
        try
        {
            Log.v("SimpleDHT", "Insert method called");
            String hashKey = genHash(key);

            Log.v("SimpleDHT", "Generated hash for "+key+" is "+hashKey);

            if(doLocalInsert)
            {
                Log.v("SimpleDHT", "doLocalInsert Insert locally ");

                FileOutputStream outputStream;
                Context currentContext = getContext();

                try {
                    outputStream =  currentContext.openFileOutput(key,Context.MODE_PRIVATE);
                    outputStream.write(val.getBytes());
                    outputStream.close();
                } catch (Exception e) {
                    Log.v("SimpleDHT", "File write failed in insert"+e);
                }
            }
            else if(joinRequest == false || ((selfNode.compareTo(hashKey) > 0) && (hashKey.compareTo(predNode.node)>0)) || ((selfNode.compareTo(hashKey) > 0) && selfNode.compareTo(predNode.node) < 0))
            {
                Log.v("SimpleDHT", "Insert locally ");

                FileOutputStream outputStream;
                Context currentContext = getContext();

                try {
                    outputStream =  currentContext.openFileOutput(key,Context.MODE_PRIVATE);
                    outputStream.write(val.getBytes());
                    outputStream.close();
                } catch (Exception e) {
                    Log.v("SimpleDHT", "File write failed in insert"+e);
                }
            }
            else
            {
                Log.v("SimpleDHT", "Go to next successor to insert ");
                String msg = "I";
                msg+="|";
                if(succNode.node.compareTo(selfNode)<0 && hashKey.compareTo(selfNode) > 0)
                {
                    msg+="1";
                }
                else
                {
                    msg+="0";
                }
                msg+="|";
                msg+=key;
                msg+="|";
                msg+=val;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"I",msg);
            }
        }
        catch (Exception e)
        {
            Log.v("SimpleDHT", "Insert Method failed "+e);
        }
    }

    public void JoinReplyWrapper(String port)
    {
        firstJoinRequest =false;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"R",port);
    }
    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        try
        {
            TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            int avd = (Integer.parseInt(portStr));
            myPort = String.valueOf(avd*2);
            Log.v("SimpleDHT", "On create called in provider "+myPort+" avd "+portStr);
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.v("SimpleDHT", "On create called in provider, serverSocket created ");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);
            selfNode = genHash(portStr);
            Log.v("SimpleDHT", "Self node is "+selfNode);
            JoinRequestReceived[0] = false;
            JoinRequestReceived[1] = false;
            JoinRequestReceived[2] = false;
            JoinRequestReceived[3] = false;
            if(myPort.equals("11108"))
            {
                availableNodes = "11108";
                portNodeMap obj = new portNodeMap();
                obj.Set(selfNode,Integer.parseInt(myPort));
                sortedNodes.add(obj);
            }
            if(!(myPort.equals("11108")))
            {
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"J");
            }

        }
        catch (Exception e)
        {
            Log.v("SimpleDHT", "Exception caught in on create provider "+e);
        }
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        try
        {

            MatrixCursor matrixCursor = new MatrixCursor(new String[] { KEY_FIELD, VALUE_FIELD });
            Log.v("SimpleDHT", "Query called "+selection);

            if(!(selection.equals("@"))&&!(selection.equals("*")))
            {
                String res = GetQueriedFile(selection);
                if(res.equals(""))
                {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"QK",myPort,selection);
                    if(queryResultReceived == false)
                    {
                        Log.v("SimpleDHT", "Query put to sleep ");
                        Thread.sleep(1000);
                    }

                    Log.v("SimpleDHT", "Query key result received : "+queryKey);

                    matrixCursor.addRow(new Object[] { selection, queryKey });

                    queryResultReceived = false;
                    return matrixCursor;
                }
                else
                {
                    Log.v("SimpleDHT", "Key value found "+res);
                    matrixCursor.addRow(new Object[] { selection, res });
                    return matrixCursor;
                }
            }
            ArrayList<String> res = GetLocalFiles();

            for(int i = 0 ; i < res.size() ; i+=2)
            {
                Log.v("SimpleDHT", "Adding local query result "+res.get(i)+","+res.get(i+1));
                matrixCursor.addRow(new Object[] { res.get(i), res.get(i+1) });
            }
            if(successorFound == true && selection.equals("*"))
            {
                Log.v("SimpleDHT", "Query * called");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"Q",myPort);
                if(queryResultReceived == false)
                {
                    Log.v("SimpleDHT", "Query put to sleep ");
                    Thread.sleep(1500);
                }

                Log.v("SimpleDHT", "Query result received");

                for(int i = 0 ; i < queryResults.size() ; i+=2)
                {
                    Log.v("SimpleDHT", "Adding query result "+queryResults.get(i)+","+queryResults.get(i+1));
                    matrixCursor.addRow(new Object[] { queryResults.get(i), queryResults.get(i+1) });
                }
                queryResults.clear();
                queryResultReceived = false;
            }

            return matrixCursor;

        }
        catch (Exception e)
        {
            Log.v("SimpleDHT", "Exception caught in query "+e);
        }
        return null;
    }

    public void QueryRequestWrapper(String port,String msg)
    {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"Q",port,msg);
    }
    public void QueryKeyRequestWrapper(String port,String key,String found,String result)
    {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"QK",port,key,found,result);
    }

    public void DeleteRequestWrapper(String port)
    {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"D",port);
    }
    public void DeleteKeyRequestWrapper(String port,String key)
    {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"QK",port,key);
    }

    public ArrayList<String> GetLocalFiles()
    {
        FileInputStream inputStream;
        Context currentContext = getContext();
        String[] keyFiles = currentContext.fileList();
        ArrayList<String> result = new ArrayList<String>();
        for(String file:keyFiles) {
            StringBuilder value = new StringBuilder();
            try {
                inputStream = currentContext.openFileInput(file);

                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    value.append(line);
                }
                inputStreamReader.close();
                inputStream.close();
            } catch (Exception e) {
                Log.v("SimpleDHT", "Query File open failed " + e);
            }
            Log.v("SimpleDHT", "Adding Key " + file + " value " + value);
            result.add(file);
            result.add(value.toString());
        }
        return result;
    }

    public boolean DeleteFile(String Key)
    {
        Context currentContext = getContext();

        try {
            if(currentContext.deleteFile(Key))
            {
                Log.v("SimpleDHT", "File deleted " + Key);
                return true;
            }
            Log.v("SimpleDHT", "File not found to delete " + Key);
            return false;
        } catch (Exception e) {
            Log.v("SimpleDHT", "To be deleted File not found " + e);
          return false;
        }
    }

    public void DeleteLocalFiles()
    {
        Log.v("SimpleDHT", "Deleting local files " );
        Context currentContext = getContext();
        String[] keyFiles = currentContext.fileList();
        for(String file:keyFiles) {
            try {
                if(currentContext.deleteFile(file))
                {
                    Log.v("SimpleDHT", "File deleted " + file);

                }
                else
                {
                    Log.v("SimpleDHT", "File not found to delete " + file);
                }
            } catch (Exception e) {
                Log.v("SimpleDHT", "Delete File open failed " + e);
            }

        }
        return ;
    }

    public String GetQueriedFile(String Key)
    {
        FileInputStream inputStream;
        Context currentContext = getContext();

            StringBuilder value = new StringBuilder();
            try {
                inputStream = currentContext.openFileInput(Key);

                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    value.append(line);
                }
                inputStreamReader.close();
                inputStream.close();

                return value.toString();
            } catch (Exception e) {
                Log.v("SimpleDHT", "Queried File not found " + e);
                return "";
            }

    }



    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    void AddNewNodes(String newNode)
    {
        try
        {
            int avd = Integer.parseInt(newNode);
            avd/=2;
            String n = genHash(Integer.toString(avd));
            portNodeMap obj = new portNodeMap();
            obj.Set(n,Integer.parseInt(newNode));
            sortedNodes.add(obj);

            Collections.sort(sortedNodes,new sortedNodesComparator());
            int len = sortedNodes.size();
            for(int i = 0 ; i < len ; i++)
            {
                Log.v("SimpleDHT", " After Sorting : "+sortedNodes.get(i).node+","+sortedNodes.get(i).port);
            }

            int prevPort;
            int succPort;
            int j;

            for( j = 0 ; j < len ;j++)
            {
                int senderPort = sortedNodes.get(j).port;
                if(senderPort == Integer.parseInt(myPort)) {
                    break;
                }
            }

            if(j == 0)
            {
                prevPort = sortedNodes.get(len-1).port;
                succPort = sortedNodes.get(1).port;
            }
            else if(j == len-1)
            {
                prevPort = sortedNodes.get(len-2).port;
                succPort = sortedNodes.get(0).port;
            }
            else
            {
                prevPort = sortedNodes.get(j-1).port;
                succPort = sortedNodes.get(j+1).port;
            }

            predNode.Set(genHash(Integer.toString(prevPort/2)),prevPort);
            succNode.Set(genHash(Integer.toString(succPort/2)),succPort);

            Log.v("SimpleDHT", "Updated Adjacent nodes for "+myPort+" are "+prevPort+","+succPort);
        }
        catch (Exception e) {
            Log.v("SimpleDHT", "Exception in adding new node "+e);
        }

    }

    class ServerTask extends AsyncTask<ServerSocket, String, Void> {


        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.v("SimpleDHT", "Server task initiated");
            try
            {
                while (true)
                {
                    Log.v("SimpleDHT", "Modified Waiting for message on server side");
                    Long millis = System.currentTimeMillis();
                    Log.v("SimpleDHT", "Waiting on server at "+millis%100000);
                    Socket clientSocket = serverSocket.accept();
                    millis = System.currentTimeMillis();
                    Log.v("SimpleDHT", "Connection accepted at "+millis%100000);

                    BufferedReader msgFromClient
                            = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

                    Log.v("SimpleDHT", "New Connection established with server");

                    String message = msgFromClient.readLine();
                    if (message == null) {
                        Log.v("SimpleDHT", "Null message on Server Side");
                        msgFromClient.close();
                        clientSocket.close();
                        continue;
                    }
                    Log.v("SimpleDHT", "Message received on server "+message);

                    String[] splitMessage = message.split("\\|");

                    if(splitMessage[0].equals("J"))
                    {
                        Log.v("SimpleDHT", "Received join request from "+splitMessage[1]);
                        int senderPort = Integer.parseInt(splitMessage[1]);


                        JoinReplyWrapper(splitMessage[1]);


                        joinRequest = true;
                        successorFound = true;
                    }
                    else if(splitMessage[0].equals("R"))
                    {
                        Log.v("SimpleDHT", "Request to join message received from 11108 "+splitMessage[1]+","+splitMessage[2]);
                        int prevPort = Integer.parseInt(splitMessage[1]);
                        int succPort = Integer.parseInt(splitMessage[2]);
                        predNode.Set(genHash(Integer.toString(prevPort/2)),prevPort);
                        succNode.Set(genHash(Integer.toString(succPort/2)),succPort);
                        joinRequest = true;
                        successorFound = true;
                    }
                    else if(splitMessage[0].equals("I"))
                    {
                        Log.v("SimpleDHT", "Received Insert request with key and value as "+splitMessage[2]+","+splitMessage[3]);
                        if(splitMessage[1].equals("1"))
                        {
                            Log.v("SimpleDHT", "No node can insert this message so inserting locally with key and value as "+splitMessage[2]+","+splitMessage[3]);
                            insertMethod(true,splitMessage[2],splitMessage[3]);
                        }
                        else
                        {
                            Log.v("SimpleDHT", "This node may insert this message with key and value as "+splitMessage[2]+","+splitMessage[3]);
                            insertMethod(false,splitMessage[2],splitMessage[3]);
                        }

                    }
                    else if(splitMessage[0].equals("Q"))
                    {
                        Log.v("SimpleDHT", "Received Query request from "+splitMessage[1]);
                        if(splitMessage[1].equals(myPort))
                        {
                            Log.v("SimpleDHT", "Query Completed ");

                            for(int i = 2 ; i < splitMessage.length ;i++)
                            {
                                Log.v("SimpleDHT", "Adding query result "+splitMessage[i]);
                                queryResults.add(splitMessage[i]);
                            }
                            queryResultReceived = true;
                        }
                        else
                        {
                            Log.v("SimpleDHT", "Query request still on going ");
                            QueryRequestWrapper(splitMessage[1],message);
                        }

                    }
                    else if(splitMessage[0].equals("QK"))
                    {
                        Log.v("SimpleDHT", "Received Query key request from "+splitMessage[1]);
                        if(splitMessage[1].equals(myPort))
                        {
                            Log.v("SimpleDHT", "Query key request Completed ");

                            queryKey = splitMessage[4];
                            queryResultReceived = true;
                        }
                        else
                        {
                            Log.v("SimpleDHT", "Query key request still on going ");
                            if(splitMessage[3].equals("0"))
                            {
                                QueryKeyRequestWrapper(splitMessage[1],splitMessage[2],splitMessage[3],"");
                            }
                            else
                            {
                                QueryKeyRequestWrapper(splitMessage[1],splitMessage[2],splitMessage[3],splitMessage[4]);
                            }
                        }

                    }
                    else if(splitMessage[0].equals("D"))
                    {
                        Log.v("SimpleDHT", "Delete all request received ");
                        if(splitMessage[1].equals(myPort))
                        {
                            Log.v("SimpleDHT", "Delete all completed ");
                        }
                        else
                        {
                            Log.v("SimpleDHT", "Delete still going on ");
                            DeleteLocalFiles();
                            DeleteRequestWrapper(splitMessage[1]);
                        }

                    }
                    else if(splitMessage[0].equals("DK"))
                    {
                        Log.v("SimpleDHT", "Delete Key request received ");
                        if(DeleteFile(splitMessage[2]))
                        {
                            Log.v("SimpleDHT", "File found and deleted ");
                        }
                        else
                        {
                            Log.v("SimpleDHT", "Delete still going on for key "+splitMessage[2]);
                            DeleteKeyRequestWrapper(splitMessage[1],splitMessage[2]);
                        }

                    }

                    Log.v("SimpleDHT", "Server side processing completed");
                    clientSocket.close();
                }
            }
            catch(Exception e)
            {
                Log.v("SimpleDHT", "Exception thrown in Serverside "+e);
            }
            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try
            {
                if(msgs[0].equals("J"))
                {
                    try {
                        String msg ="J";
                        msg+="|";
                        msg+=myPort;
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt("11108"));
                        OutputStream os;
                        PrintWriter pw;
                        os= socket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(msg);
                        Thread.sleep(200);
                        pw.flush();
                        socket.close();
                        Log.v("SimpleDHT", "new message sent to 11108");
                    }
                    catch (Exception e) {
                        Log.v("SimpleDHT", "Exception in Join send");
                    }
                }
                else if(msgs[0].equals("R"))
                {
                    AddNewNodes(msgs[1]);

                    availableNodes+="|";
                    availableNodes+=msgs[1];
                    Log.v("SimpleDHT", "New nodes added, available nodes "+availableNodes);
                        try {

                            int t = 0;
                            int len = sortedNodes.size();
                            for (Iterator<portNodeMap> iterator = sortedNodes.iterator(); iterator.hasNext(); ) {
                                portNodeMap element = iterator.next();
                                if(element.port != 11108)
                                {
                                    int reqNode = element.port;
                                    int prevPort;
                                    int succPort;

                                    if(t == 0)
                                    {
                                        prevPort = sortedNodes.get(len-1).port;
                                        succPort = sortedNodes.get(1).port;
                                    }
                                    else if(t == len-1)
                                    {
                                        prevPort = sortedNodes.get(len-2).port;
                                        succPort = sortedNodes.get(0).port;
                                    }
                                    else
                                    {
                                        prevPort = sortedNodes.get(t-1).port;
                                        succPort = sortedNodes.get(t+1).port;
                                    }

                                    String msg ="R";
                                    msg+="|";
                                    msg+=Integer.toString(prevPort);
                                    msg+="|";
                                    msg+=Integer.toString(succPort);
                                    Log.v("SimpleDHT", "Sending join Message to "+reqNode+" msg : "+msg);

                                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                            reqNode);
                                    OutputStream os;
                                    PrintWriter pw;
                                    os= socket.getOutputStream();
                                    pw= new PrintWriter(os,true);
                                    pw.println(msg);
                                    Thread.sleep(200);
                                    pw.flush();
                                    socket.close();

                                }
                                t++;
                            }



                        }
                        catch (Exception e)
                        {
                            Log.v("SimpleDHT", "Error in Sending join reply message "+e);
                    }
                }
                else if(msgs[0].equals("I"))
                {
                    if(successorFound)
                    {
                        Log.v("SimpleDHT", "Sending Insert message to "+succNode.port+" with msg : "+msgs[1]);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                succNode.port);
                        OutputStream os;
                        PrintWriter pw;
                        os= socket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(msgs[1]);
                        Thread.sleep(200);
                        pw.flush();
                        socket.close();
                    }
                    else
                    {
                        Log.v("SimpleDHT", "Successor not yet found, trying to send "+msgs[1]);
                    }
                }

                else if(msgs[0].equals("Q"))
                {
                    Log.v("SimpleDHT", "Received query request from "+msgs[1]);
                    if(msgs[1].equals(myPort))
                    {
                        String message = "Q";
                        message+="|";
                        message+=myPort;
                        message+="|";
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                succNode.port);
                        Log.v("SimpleDHT", "Sending initial query request to  "+succNode.port+","+message);
                        OutputStream os;
                        PrintWriter pw;
                        os= socket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(message);
                        Thread.sleep(200);
                        pw.flush();
                        socket.close();
                    }
                    else
                    {
                        String message = "";
                        message+=msgs[2];
                        ArrayList<String> temp = GetLocalFiles();
                        for(int i = 0 ; i < temp.size();i+=2)
                        {
                            message+=temp.get(i);
                            message+="|";
                            message+=temp.get(i+1);
                            message+="|";
                        }

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                succNode.port);

                        Log.v("SimpleDHT", "Sending next query request to  "+succNode.port+","+message);
                        OutputStream os;
                        PrintWriter pw;
                        os= socket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(message);
                        Thread.sleep(200);
                        pw.flush();
                        socket.close();
                    }
                }
                else if(msgs[0].equals("QK"))
                {
                    if(msgs[1].equals(myPort))
                    {
                        String message = "QK";
                        message+="|";
                        message+=myPort;
                        message+="|";
                        message+=msgs[2];
                        message+="|";
                        message+="0";
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                succNode.port);
                        Log.v("SimpleDHT", "Sending initial query key request to  "+succNode.port+","+message);
                        OutputStream os;
                        PrintWriter pw;
                        os= socket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(message);
                        Thread.sleep(200);
                        pw.flush();
                        socket.close();
                    }
                    else
                    {
                        String message = "QK";
                        message+="|";
                        message+=msgs[1];
                        message+="|";
                        message+=msgs[2];
                        message+="|";
                        int port;
                        String res = GetQueriedFile(msgs[2]);
                        if(res.equals(""))
                        {
                            Log.v("SimpleDHT", "Could not find queried key's value "+msgs[2]);
                            message+="0";
                            port = succNode.port;
                        }
                        else
                        {
                            Log.v("SimpleDHT", "Found queried key's value "+msgs[2]+","+res);
                            message+="1";
                            message+="|";
                            message+=res;
                            port = Integer.parseInt(msgs[1]);
                        }

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                port);

                        Log.v("SimpleDHT", "Sending query key request to  "+port+","+message);
                        OutputStream os;
                        PrintWriter pw;
                        os= socket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(message);
                        Thread.sleep(200);
                        pw.flush();
                        socket.close();
                    }
                }
                else if(msgs[0].equals("D"))
                {

                        String message="D";
                        message+="|";
                        message+=msgs[1];
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                succNode.port);
                        Log.v("SimpleDHT", "Sending Delete all request to  "+succNode.port+","+message);
                        OutputStream os;
                        PrintWriter pw;
                        os= socket.getOutputStream();
                        pw= new PrintWriter(os,true);
                        pw.println(message);
                        Thread.sleep(200);
                        pw.flush();
                        socket.close();
                }
                else if(msgs[0].equals("DK"))
                {
                    String message="DK";
                    message+="|";
                    message+=msgs[1];
                    message+="|";
                    message+=msgs[2];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            succNode.port);
                    Log.v("SimpleDHT", "Sending Delete Key request to  "+succNode.port+","+message);
                    OutputStream os;
                    PrintWriter pw;
                    os= socket.getOutputStream();
                    pw= new PrintWriter(os,true);
                    pw.println(message);
                    Thread.sleep(200);
                    pw.flush();
                    socket.close();
                }
            }

            catch (Exception e)
            {
                Log.v("SimpleDHT", "Exception inside client task "+e);
            }
            return null;
        }

    }



}
