package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class StorageNode {

    private ServerSocket srvSocket;
    //private static String path = "./home2/slerkpatomsak";
    private static String path  = "./Bass00";
    private Boolean checkPoint = true;
    private static String controllerHostname = null;
    private static int controllerPort = 28000;

    private static Map<String,Integer> storageNodeList = new HashMap<>(); //storage node list for store chunk
    private static List<String> updatedChunkList = new ArrayList<>();
    private static Map<String,String> chunkSumList = new HashMap<>();

    public static void main(String[] args)
            throws Exception {
        String hostname = getHostname();
        System.out.println("Starting storage node on " + hostname + "...");
        new StorageNode().start();
    }

    public void start()
            throws Exception {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Please input port : ");
        String port = scanner.nextLine();
        System.out.print("Where is controllerSocket?(hostname) : ");
        controllerHostname = scanner.nextLine();
        srvSocket = new ServerSocket(Integer.parseInt(port));
        System.out.println("Listening...");
        Socket socket = null;
        sendHeartBeatMessage(socket);

        while (true) {
            socket = srvSocket.accept();
            StorageMessages.StorageMessageWrapper msgWrapper
                    = StorageMessages.StorageMessageWrapper.parseDelimitedFrom(
                    socket.getInputStream());
            System.out.println(msgWrapper);
            if (msgWrapper != null) {
                if (msgWrapper.hasStoreChunkMsg()) {
                    StorageMessages.StoreChunk storeChunkMsg = msgWrapper.getStoreChunkMsg();
                    storeChunk(storeChunkMsg);
                    distributeChunk(socket, storeChunkMsg);
                } else if (msgWrapper.hasRetrieveFileMsg()) {
                    StorageMessages.RetrieveFile retrieveFileMsg = msgWrapper.getRetrieveFileMsg();
                    retrieveFile(socket, retrieveFileMsg);
                } else if (msgWrapper.hasNodeFailureMsg()) {
                    StorageMessages.NodeFailure nodeFailure = msgWrapper.getNodeFailureMsg();
                    copyReplica(socket,nodeFailure);
                }
            }
        }
    }

    private void createHeartbeatMessage(Socket socket) throws IOException {
        socket = new Socket(controllerHostname,controllerPort);
        List<String> chunkList = null;
        if(checkPoint.equals(true)) {
            chunkList = getChunkList();
        }
        else {
            chunkList = updatedChunkList;
        }
        ControllerMessages.HeartBeat heartBeat = ControllerMessages
                .HeartBeat
                .newBuilder()
                .setHostName(getHostname() + path)
                .setPort(srvSocket.getLocalPort())
                .setFreeSpace(getFreeSpace())
                .addAllChunkList(chunkList)
                .build();
        ControllerMessages.ControllerMessageWrapper controllerMessageWrapper = ControllerMessages
                .ControllerMessageWrapper
                .newBuilder().setHeartBeatMsg(heartBeat).build();

        controllerMessageWrapper.writeDelimitedTo(socket.getOutputStream());
        updatedChunkList.clear();
    }

    private void sendHeartBeatMessage(final Socket socket){
        Timer t = new Timer();
        t.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    createHeartbeatMessage(socket);
                    checkPoint = false;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, 0, 5000);
    }

    private static List<String> getChunkList(){
        List<String> fileList = new ArrayList<>();
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                fileList.add(listOfFiles[i].getName());
            }
        }
        return fileList;
    }

    private static void setStorageNodeList(StorageMessages.StoreChunk storeChunkMsg) throws UnknownHostException {

        for(int i = 0; i< storeChunkMsg.getStoreNodesList().size(); i++){
                storageNodeList.put(storeChunkMsg.getStoreNodesList().asByteStringList().get(i).toStringUtf8()
                        ,storeChunkMsg.getStoreNodesPortList().get(i));
        }
    }

    private static void  storeChunk(StorageMessages.StoreChunk storeChunkMsg) throws NoSuchAlgorithmException, IOException {
        Boolean boo = true;
        chunkSumList.put(storeChunkMsg.getFileName()+"."+storeChunkMsg.getChunkId(),storeChunkMsg.getCheckSum());
        StoringRetrievingChunk storingRetrievingChunk = new StoringRetrievingChunk();
        boo = storingRetrievingChunk.storeChunk(storeChunkMsg,path);
        updatedChunkList.add("+"+storeChunkMsg.getFileName()+"."+storeChunkMsg.getChunkId());
    }

    private static void distributeChunk(Socket socket, StorageMessages.StoreChunk message) throws IOException {
        System.out.print("1");
        setStorageNodeList(message);
        Map<String,Integer> temp = new HashMap<>();
        temp.putAll(storageNodeList);

            if (temp.containsKey(getHostname()+path) && temp.size()>1) {
                    temp.remove(getHostname() + path);
                    socket = new Socket(subString(temp.keySet().iterator().next()),temp.values().iterator().next());
                    StorageMessages.StoreChunk storeChunkMsg
                            = StorageMessages.StoreChunk.newBuilder()
                            .setFileName(message.getFileName())
                            .setChunkId(message.getChunkId())
                            .setReplica(message.getReplica())
                            .setData(message.getData())
                            .addAllStoreNodes(temp.keySet())
                            .addAllStoreNodesPort(temp.values())
                            .setCheckSum(message.getCheckSum())
                            .build();

                    StorageMessages.StorageMessageWrapper msgWrapper =
                            StorageMessages.StorageMessageWrapper.newBuilder()
                                    .setStoreChunkMsg(storeChunkMsg)
                                    .build();
                System.out.print(msgWrapper);
                    msgWrapper.writeDelimitedTo(socket.getOutputStream());

                }
                System.out.print("2");

    }

    private static void  retrieveFile(Socket socket, StorageMessages.RetrieveFile retrieveFileMsg) throws IOException, NoSuchAlgorithmException {
            String chunkName = retrieveFileMsg.getFileName() + "." + retrieveFileMsg.getChunkId();
            StoringRetrievingChunk storingRetrievingChunk = new StoringRetrievingChunk();

            String checkSum = storingRetrievingChunk.getCheckSum(new File(path, chunkName));
            RetrievingMessages.RetrievingMessageWrapper messageWrapper = null;

            if((chunkSumList.get(chunkName)).equals(checkSum)){
                byte[] bytes = storingRetrievingChunk.retrieveFile(chunkName,path);
                updatedChunkList.add("-" + retrieveFileMsg.getFileName() + "." + retrieveFileMsg.getChunkId());
                if (bytes != null) {
                    RetrievingMessages.RetrieveChunk retrieveChunk =
                            RetrievingMessages
                                    .RetrieveChunk
                                    .newBuilder()
                                    .setChunkId(retrieveFileMsg.getChunkId())
                                    .setFileName(retrieveFileMsg.getFileName())
                                    .setData(ByteString.copyFrom(bytes))
                                    .build();

                    messageWrapper = RetrievingMessages.RetrievingMessageWrapper
                            .newBuilder().setRetrieveChunkMsg(retrieveChunk).build();
                } else {
                    RetrievingMessages.UnfoundedChunk unfoundedChunk = RetrievingMessages
                            .UnfoundedChunk
                            .newBuilder()
                            .setMsg(retrieveFileMsg.getFileName() + "." + retrieveFileMsg.getChunkId() + "not found")
                            .build();
                    messageWrapper = RetrievingMessages.RetrievingMessageWrapper
                            .newBuilder().setUnfoundedChunkMsg(unfoundedChunk).build();
                }
                System.out.println(messageWrapper);
                messageWrapper.writeDelimitedTo(socket.getOutputStream());
               // storingRetrievingChunk.deleteChunk(retrieveFileMsg, path);

            }
            else {
                System.out.print(chunkName + " is corrupted");
                socket = new Socket(controllerHostname,controllerPort);
              ControllerMessages.ChunkFailure chunkFailure =
                      ControllerMessages
                              .ChunkFailure
                              .newBuilder()
                              .addChunkName(chunkName)
                      .build();
              ControllerMessages.ControllerMessageWrapper wrapper =
                      ControllerMessages.ControllerMessageWrapper.newBuilder()
                      .setChunkFailureMsg(chunkFailure)
                      .build();
              wrapper.writeDelimitedTo(socket.getOutputStream());
              System.out.print(wrapper);
            }



        }

    private static void copyReplica(Socket socket, StorageMessages.NodeFailure nodeFailure) throws IOException, NoSuchAlgorithmException {
        Map<String,Integer> chunkReplica = new HashMap<>();
        for(int i=0;i<nodeFailure.getChunkNameListCount(); i++){
            chunkReplica.put(nodeFailure.getChunkNameList(i),nodeFailure.getChunkAmount(i));
        }

        ArrayList<String> nodes = new ArrayList<>();
        nodes.addAll(nodeFailure.getStoreNodesList());

        ArrayList<Integer> ports = new ArrayList<>();
        ports.addAll(nodeFailure.getStoreNodesPortList());

        StoringRetrievingChunk bytesData = new StoringRetrievingChunk();

        for(String chunk : chunkReplica.keySet()) {
            StorageMessages.StoreChunk storeChunkMsg
                    = StorageMessages.StoreChunk.newBuilder()
                    .setFileName(getChunkNameId(chunk).get(0))
                    .setChunkId(Integer.parseInt(getChunkNameId(chunk).get(1)))
                    .setReplica(chunkReplica.get(chunk))
                    .setData(ByteString.copyFrom(bytesData.retrieveFile(chunk, path)))
                    .setCheckSum(chunkSumList.get(chunk))
                    .addAllStoreNodes(nodes)
                    .addAllStoreNodesPort(ports)
                    .build();
            System.out.print("hello");
            distributeChunk(socket,storeChunkMsg);
            System.out.print("world");
        }
    }

    private static ArrayList<String> getChunkNameId(String chunkId){
        String[] temp  = chunkId.split("\\.");
        ArrayList<String> name = new ArrayList<>();
        if(temp.length == 2){
            name.add(temp[0]);
            name.add(temp[1]);
        }
        else {
            name.add(temp[0]+"."+temp[1]);
            name.add(temp[temp.length-1]);
        }
        return name;
    }

    private static String subString(String hostname){
        String arrOfStr = hostname.substring(0,25);
        return arrOfStr;
    }

    /**
     * Retrieves the short host name of the current host.
     *
     * @return name of the current host
     */
    private static String getHostname() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostName();
    }
    private static long getFreeSpace(){
        File file = new File(path);
        return file.getFreeSpace();
    }

}
