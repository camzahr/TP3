import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest {

    private static Configuration conf = null;
    /**
     * Initialization
     */
    static {
        conf = HBaseConfiguration.create();
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    /**
     * Create a table
     */
    public static void creatTable(String tableName, String[] familys)
            throws Exception {
        //Start Configuration
        HBaseAdmin admin = new HBaseAdmin(conf);
        //Check if table exist
        if (admin.tableExists(tableName)) {
            //If y, do nothing
            System.out.println("table already exists!");
        } else {
            //Create table with the name given
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            //Try to disable and delete the table
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            //try to put the new user
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            //Say its ok
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        //Try to delete the user we want
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }

    /**
     * Get a row
     */
    public static void getOneRecord (String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        //use get for getting a single row of our table
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
    }
    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try{
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            //Scan all the records
            ResultScanner ss = table.getScanner(s);
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                    //Print each info
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        } catch (IOException e){
            System.out.print("Error 404 : not found. Please try again");
            e.printStackTrace();
        }
    }

    public static void main(String[] agrs) {
        try {
            //Create table with name
            String tablename = "myNetwork";
            String[] familys = { "info", "friends" };
            HBaseTest.creatTable(tablename, familys);

            //Try to record some infos
            System.out.println("Tests Incoming...");

            // add record pierre
            HBaseTest.addRecord(tablename, "pierre", "info", "age", "24");
            HBaseTest.addRecord(tablename, "pierre", "info", "email", "pierre@adaltas.com");
            HBaseTest.addRecord(tablename, "pierre", "friends", "ninon", "1990-11-26");
            HBaseTest.addRecord(tablename, "pierre", "friends", "jeanne", "1995-03-01");
            // add record jean
            HBaseTest.addRecord(tablename, "jean", "info", "age", "35");
            HBaseTest.addRecord(tablename, "jean", "info", "email", "jean@gmail.com");
            HBaseTest.addRecord(tablename, "jean", "friends", "pierre", "1990-11-26");
            HBaseTest.addRecord(tablename, "jean", "friends", "jeanne", "1995-03-01");

            //Try to do some actions on the database
            System.out.println("===========get one record========");
            HBaseTest.getOneRecord(tablename, "pierre");

            System.out.println("===========show all record========");
            HBaseTest.getAllRecord(tablename);

            System.out.println("===========del one record========");
            HBaseTest.delRecord(tablename, "jean");
            HBaseTest.getAllRecord(tablename);

            System.out.println("===========show all record========");
            HBaseTest.getAllRecord(tablename);

            int type;
            Scanner scanner = new Scanner(System.in);

            //OUr menu
            do {

                System.out.println("Welcome to the New Facebook");
                System.out.println("1. Add");
                System.out.println("2. Show All");
                System.out.println("3. Delete One");
                System.out.println("4. Show One");
                System.out.println("0. Press 0 to exit");
                type = scanner.nextInt();

                switch (type) {
                    //Add a user
                    case 1:
                        String name;
                        String email;
                        String age;
                        String friend;
                        String date;

                        System.out.println("Type name");
                        name = scanner.next();
                        System.out.println("Type email");
                        email = scanner.next();
                        System.out.println("Type age");
                        age = scanner.next();
                        HBaseTest.addRecord(tablename, name, "info", "age", age);
                        HBaseTest.addRecord(tablename, name, "info", "email", email);

                        System.out.println("Who is your best friend ?");
                        friend = scanner.next();
                        System.out.println("What is his birth date ? (aaa-mm-dd)");
                        date = scanner.next();

                        HBaseTest.addRecord(tablename, name, "friends", friend, date);

                        break;

                    //Show all users
                    case 2:
                        HBaseTest.getAllRecord(tablename);
                        break;

                    //Delete a user
                    case 3:
                        String currentName;
                        HBaseTest.getAllRecord(tablename);
                        System.out.println("Which one do you want to delete ?");
                        currentName = scanner.next();
                        try{
                            HBaseTest.delRecord(tablename, currentName);
                        }catch (IOException e){
                            e.printStackTrace();
                            System.out.print("Error 404 : not found. Please try again");
                        }

                        System.out.println("Well done ! You delete "+currentName);

                        HBaseTest.getAllRecord(tablename);
                        break;

                    //See one user
                    case 4:
                        String oneName;
                        System.out.println("Which one do you want to see ? ");
                        oneName = scanner.next();
                        HBaseTest.getOneRecord(tablename,oneName);
                        break;

                    case 0:
                        System.out.println("Good Bye !");
                        break;

                    default:
                        System.out.println("I don't understand, try again !");
                        break;
                }

                System.out.println("Good choice !");
                for (int i = 0; i < 5; ++i) System.out.println();
            } while (type != 0);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}