import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/*BPlusTree class to store various details regarding each block of the file like,
 *  the key values and pointers to connecting blocks*/
@SuppressWarnings("serial")
class BPlusTree implements Serializable {
	public List<String> key; //ArrayList to store the key of the records
	public List<BPlusTree> blockPtr; //ArrayList to store the block pointers of intermediate blocks to corresponding blocks
	public BPlusTree parent; //stores the pointer to the parent of the current block
	public List<Long> offsetval; //ArrayList to store the offset of the records present in the input text data file
	public BPlusTree rightBlockPtr; //pointer of right block of the current node 
	public BPlusTree leftBlockPtr; //pointer of right block of the current node 
	public boolean isLeafBlock; //determines if the current block is a leaf block or intermediate block
	
	public BPlusTree() {
		this.key = new ArrayList<String>();
		this.blockPtr = new ArrayList<BPlusTree>();
		this.parent = null;
		this.offsetval = new ArrayList<Long>();
		this.rightBlockPtr = null;
		this.leftBlockPtr = null;
		this.isLeafBlock = false;
	}
}

public class BPlusTreeDB {
	
	static BPlusTree root;//root block of the index file
	static int MaxBlockSize;//max block size of each block
	static int splitIndex;//index at which we must split the current block once it reaches the maxBlockSize
	
	/**
	 * Create the index file using the B plus tree algorithm used to search records faster
	 *
	 * @param inputFile : path to the input text data file which needs to be indexed for faster access
	 * @param indexFile : path where the index file needs to be generated
	 * @param keyLength : length of the key to be considered while preparing the index file
	 * */
	public static void createIndex(String inputFile, String indexFile, int keyLength) throws IOException{
		
		long offset = 0l;// to calculate the offset of the records in the text input file
		MaxBlockSize = (1024 - keyLength) / keyLength + 8 ; // assuming each block of 1024 bytes and offset of long data type, hence 8 bytes
		splitIndex = (MaxBlockSize%2==0)?(MaxBlockSize/2)-1 : MaxBlockSize/2;
		
		//read the input text file
		File file = new File(inputFile); 
        FileInputStream fileStream = new FileInputStream(file); 
        InputStreamReader input = new InputStreamReader(fileStream); 
        BufferedReader reader = new BufferedReader(input);
		String line="",key = "";
		
		while((line = reader.readLine()) != null){ //read each line of the input text file
			key = line.substring(0,keyLength); //extract key upto the specified keyLength from each line to insert in the index file
			insertRecord(root,key, offset); // insert each new key offset pair of the record read from input file to the index file
			offset += line.length() + 2; //add 2 to each offset after adding line.length() for "\n" i.e. newline charaters
		}
		reader.close(); 
		writefile(keyLength, inputFile, indexFile);
	
	}
	
	/**
	 * Write the prepared index file object to index file
	 *
	 * @param datafilepath : path to the input text data file which needs to be indexed for faster access
	 * @param indexfilepath : path where the index file needs to be generated
	 * @param key : length of the key to be considered while preparing the index file
	 * */
	private static void writefile(int key, String datafilepath, String indexfilepath) throws IOException {
		FileOutputStream fout = new FileOutputStream(indexfilepath);
		byte[] inputFileName = datafilepath.getBytes();
		byte[] keyLength = (key+"").getBytes();
		byte[] rootOffset = (" " + root.key.get(0)).getBytes();
		FileChannel fc = fout.getChannel();
		fc.write(ByteBuffer.wrap(inputFileName));//write the input text file name from 0-255 bytes
		fc.write(ByteBuffer.wrap(keyLength), 257l);//write the keyLength of int data type in next 4 bytes
		fc.write(ByteBuffer.wrap(rootOffset), 260l);//write the offset of the root block of long data type next
		fc.position(1025l);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(root);
		oos.close();
	}
	
	/**
	 * Insert each record read from the input text file to the block object
	 *
	 * @param block : block object of BPlusTree type to insert the record
	 * @param offset : position of the record present in the input text data file
	 * @param key : key of the record to be inserted in the index file
	 * */
	public static void insertRecord(BPlusTree block, String key, long offset)throws IOException{
		
		//insert the first record in the block
		if (block == root && (block == null || block.key.isEmpty())) {
			block.key.add(key);
			block.offsetval.add((Long) offset);
			block.isLeafBlock = true;
			root = block;
			return;
		}
		
		/* Atleast one record is present in the block and
		now the current record needs to be inserted in the correct position*/
		else if (block != null || !block.key.isEmpty()) {
			
			for(int i=0;i<block.key.size();i++){
				
				String keyPresent = block.key.get(i);
				
				if(key.compareTo(keyPresent)<0){
					
					if(block.isLeafBlock){
						
						block.key.add(i, key);
						block.offsetval.add(i, offset);
						if(block.key.size()==MaxBlockSize){
							split(block);
						}
						return;
					}
					
					//current block is an intermediate block and key to be inserted is lesser than this so,
					//traverse to the left sub-block
					else{
						insertRecord(block.blockPtr.get(i), key, offset);
						return;
					}
				}
				
				else if(key.compareTo(keyPresent)>0){
					
					//Key to be inserted is greater than the key at this position. 
					//So continue searching for the correct position.
					if (i < block.key.size() - 1) {
						continue;
					}
					
					else{
						if (block.isLeafBlock){
							block.key.add(key);
							block.offsetval.add(offset);
							//split the block if the size of the block is exhausted
							if(block.key.size()==MaxBlockSize){
								split(block);
								return;
							}
							return;
						}
						
						//current block is an intermediate block and key to be inserted is greater than this so,
						//traverse to the left sub-block
						else{
							insertRecord(block.blockPtr.get(i + 1),key, offset);
							return;
						}
					}
				}
				
				else{
					System.out.println("Record already exists!");
					return;
				}
			}
			
		}
	}
	
	/**
	 * Split the block once it reaches the maxBlockSize
	 * @param block : block object of BPlusTree type to insert the record
	 * */
	public static void split(BPlusTree block){
		
		BPlusTree leftBlock = new BPlusTree();
		BPlusTree rightBlock = new BPlusTree();
		BPlusTree tempBlock = new BPlusTree();
		
		if(block.isLeafBlock){
			
			for(int i = 0;i<=splitIndex;i++){
				leftBlock.key.add(block.key.get(i));
				leftBlock.offsetval.add(block.offsetval.get(i));
			}
			
			for(int i=splitIndex+1;i<block.key.size();i++){
				rightBlock.key.add(block.key.get(i));
				rightBlock.offsetval.add(block.offsetval.get(i));
			}
			
			leftBlock.isLeafBlock = true;
			rightBlock.isLeafBlock = true;
			
			leftBlock.rightBlockPtr = rightBlock;
			leftBlock.leftBlockPtr = block.leftBlockPtr;
			
			rightBlock.leftBlockPtr = leftBlock;
			rightBlock.rightBlockPtr = block.rightBlockPtr;
			
			if(block.parent==null){
				tempBlock.blockPtr.add(leftBlock);
				tempBlock.blockPtr.add(rightBlock);
				tempBlock.key.add(rightBlock.key.get(0));
				
				leftBlock.parent = tempBlock;
				rightBlock.parent = tempBlock;
				
				root = tempBlock;
				block = tempBlock;
			}
			
			else{
				tempBlock = block.parent;
				String splitKey = rightBlock.key.get(0);
				
				for(int i = 0;i<tempBlock.key.size();i++){
					if(splitKey.compareTo(tempBlock.key.get(i))<=0){
						tempBlock.key.add(i, splitKey);
						tempBlock.blockPtr.add(i, leftBlock);
						tempBlock.blockPtr.set(i+1, rightBlock);
						break;
					}
					else{
						if (i < tempBlock.key.size() - 1) {
							continue;
						}
						else{
							tempBlock.key.add(splitKey);
							tempBlock.blockPtr.add(i+1, leftBlock);
							tempBlock.blockPtr.set(i+2, rightBlock);
							break;
						}
					}
				}
				
				if (block.leftBlockPtr != null) {
					block.leftBlockPtr.rightBlockPtr = leftBlock;
					leftBlock.leftBlockPtr = block.leftBlockPtr;
				}
				if (block.rightBlockPtr != null) {
					block.rightBlockPtr.leftBlockPtr = rightBlock;
					rightBlock.rightBlockPtr = block.rightBlockPtr;
				}
				
				leftBlock.parent = tempBlock;
				rightBlock.parent = tempBlock;
				
				//split the parent block also if the size of the block is exhausted				
				if (tempBlock.key.size() == MaxBlockSize) {
					split(tempBlock);
					return;
				}
				return;
			}
		}//end of leafBlock
		
		else{
			
			String splitKey = block.key.get(splitIndex);
			int i = 0,k=0;
			for(i=0;i<=splitIndex;i++){
				leftBlock.key.add(block.key.get(i));
				leftBlock.blockPtr.add(block.blockPtr.get(i));
				leftBlock.blockPtr.get(i).parent = leftBlock;
			}
			leftBlock.blockPtr.add(block.blockPtr.get(i+1));
			leftBlock.blockPtr.get(i+1).parent = leftBlock;
			
			for(i=splitIndex+2;i<block.key.size();i++){
				rightBlock.key.add(block.key.get(i));
				rightBlock.blockPtr.add(block.blockPtr.get(i));
				rightBlock.blockPtr.get(k++).parent = rightBlock;
			}
			rightBlock.blockPtr.add(block.blockPtr.get(i+1));
			rightBlock.blockPtr.get(k++).parent = rightBlock;
			
			if(block.parent==null){
				tempBlock.blockPtr.add(leftBlock);
				tempBlock.blockPtr.add(rightBlock);
				tempBlock.key.add(splitKey);
				
				leftBlock.parent = tempBlock;
				rightBlock.parent = tempBlock;
				
				root = tempBlock;
				block = tempBlock;
			}
			
			else{
				tempBlock = block.parent;
				
				for(i = 0;i<tempBlock.key.size();i++){
					if(splitKey.compareTo(tempBlock.key.get(i))<=0){
						tempBlock.key.add(i, splitKey);
						tempBlock.blockPtr.add(i, leftBlock);
						tempBlock.blockPtr.set(i+1, rightBlock);
						break;
					}
					else{
						if (i < tempBlock.key.size() - 1) {
							continue;
						}
						
						else{
							tempBlock.key.add(splitKey);
							tempBlock.blockPtr.add(i+1, leftBlock);
							tempBlock.blockPtr.set(i+2, rightBlock);
							break;
						}
					}
				}
				
				leftBlock.parent = tempBlock;
				rightBlock.parent = tempBlock;

				//split the parent block also if the size of the block is exhausted				
				if (tempBlock.key.size() == MaxBlockSize) {
					split(tempBlock);
					return;
				}
			
				return;
			}
		}
		
	}
	
	/**
	 * To find if the record exists in the file and list the subsequent records,
	 * if operation chosen is list and number of records to list is specified
	 *
	 * @param indexFile : path where the index file is generated
	 * @param key : key of the record to be searched in the index file
	 * @param numOfRecords : Number of subsequent records to be listed
	 * @param block : root block object of BPlusTree type to start the search
	 * @param insertFind : flag to find the record but not print the result in console
	 * @return : 0 if record not found and 1 if record found
	 * */
	public static int listRecords(String indexFile, String key, int numOfRecords,BPlusTree block, boolean insertFind) throws IOException, ClassNotFoundException{
		
		String[] metaData = getMetaData(indexFile);
		
		String textFileName = metaData[0];
		int keyLength = Integer.parseInt(metaData[1]);
		int currentKeyLength = key.length();
		
		//	if specified keyLength is small, then pad with blank to match the keylength used in the index file,
		//	if specified keyLength is large, then trim to match the keylength used in the index file
		if(currentKeyLength<keyLength){
			for(int i = 0;i<keyLength-currentKeyLength;i++){
				key+=" ";
			}
		}
		else{
			key = key.substring(0, keyLength);
		}
		
		int recordsListed = 1;//counter to count the number of records listed
		
		for(int i = 0;i<block.key.size();i++){
			if(!block.isLeafBlock){
				if(key.compareTo(block.key.get(i))<0){
					return listRecords(indexFile, key, numOfRecords, block.blockPtr.get(i),insertFind);
					 
				}
				else{
					if (i < block.key.size() - 1) {
						continue;
					}

					else if (i == block.key.size() - 1) {
						if (!block.isLeafBlock && block.blockPtr.get(i + 1) != null) {
							return listRecords(indexFile, key, numOfRecords,block.blockPtr.get(i + 1),insertFind);
						}
					}
				}
			}
			//We have traversed and reached the leaf block where we can find the key
			else{
				
				if(block.key.indexOf(key)==-1){
					if(!insertFind){
						System.out.println("Given key doesn't exist.");
					}
					if(numOfRecords==1){
						return 0;
					}
					
				}
				while(block!=null && recordsListed<=numOfRecords){
					for(int j = 0;j<block.key.size();j++){
						if(key.compareTo(block.key.get(j))>0){
							if (j < block.key.size() - 1) {
								continue;
							}
							else{
								block = block.blockPtr.get(j+1);
							}
						}
						else{
							
							long offsetVal = block.offsetval.get(j);
							
							if(!insertFind){
								readTextFileRecord(textFileName, offsetVal);
							}
							
							if(recordsListed == numOfRecords){
								return 1;
							}
							recordsListed++;
						}
					}
					block = block.rightBlockPtr;
				}
			}
		}
		return 0;
	}
	
	/**
	 * To read records from the input text file using the offset read from the generated index file 
	 *
	 * @param textFileName : path of the input text file
	 * @param offsetVal : offset of the required record to be read from the input text file
	 * */
	public static void readTextFileRecord(String textFileName, long offsetVal) throws IOException{
		
		RandomAccessFile textFile = new RandomAccessFile(textFileName, "r");
		textFile.seek(offsetVal);
		String line = textFile.readLine();
		
		System.out.println("At "+offsetVal+", record: "+line);
		textFile.close();
	}
	
	/**
	 * Insert a new record to the text data file and also add its key to the index file
	 *
	 * @param indexFile : path where the index file is generated
	 * @param data : new data which should be inserted in text file
	 * @param block : root block object of BPlusTree type to insert the record
	 * */
	public static void insertNewData(String indexFile, String data, BPlusTree block) throws IOException, ClassNotFoundException{
		
		String[] metaData = getMetaData(indexFile);
		
		String textFileName = metaData[0];
		int keyLength = Integer.parseInt(metaData[1]);
		
		int dataExists = listRecords(indexFile, data.substring(0, keyLength), 1, block, true);
		
		if(dataExists==1){
			System.out.println("Given data already exists in text input file!");
			return;
		}
		
		else{
			
			//insert the record at the end of the text input file
			File inputTextFile = new File(textFileName);
			int offset = 0;
			if (inputTextFile.exists())
				offset = (int) inputTextFile.length();
			RandomAccessFile inputFile = new RandomAccessFile(inputTextFile, "rw");
			inputFile.seek(offset);
			inputFile.writeBytes("\r\n");
			offset+=2;
			inputFile.writeBytes(data);
			//System.out.println("Record inserted successfully!");
			inputFile.close();
			
			//to fetch the root of the block from the index file
			FileInputStream fin = new FileInputStream(indexFile);
			FileChannel fc = fin.getChannel();
			fc.position(1025l);
			ObjectInputStream ois = new ObjectInputStream(fin);
			BPlusTree newRoot = (BPlusTree) ois.readObject();
			ois.close();
			root = newRoot;
			
			insertRecord(root, data.substring(0, keyLength), offset);
			
			writefile(keyLength, textFileName, indexFile);
		}
		
	}
	
	/**
	 * Get Metadata like text file name and key length used while creating the index file from the index file
	 *
	 * @param indexFile : path where the index file is generated
	 * @return the string array having the textfilename and keylength fetched from the index file
	 * */
	public static String[] getMetaData(String indexFile) throws IOException{

		RandomAccessFile randomIndexFile = new RandomAccessFile(indexFile, "r");
		//fetch the name of the input text file from the metadata we added in the index file
		randomIndexFile.seek(0);
		byte[] b = new byte[256];
		randomIndexFile.read(b);
		String textFileName = (new String(b)).trim();
		//fetch the key length from the metadata we added in the index file 
		randomIndexFile.seek(256);
		byte[] keyLengthBuffer = new byte[4];
		randomIndexFile.read(keyLengthBuffer);
		String keyLength = new String(keyLengthBuffer);
		keyLength = keyLength.trim();
		randomIndexFile.close();
		
		String[] metData = {textFileName,keyLength};
		return metData;
	}
	
	/**
	 * Helper function to fetch the root block from the generated index file
	 *
	 * @param indexFile : path where the index file is generated
	 * @return the root block
	 * */
	public static BPlusTree getRoot(String indexFile) throws IOException, ClassNotFoundException{
		
		FileInputStream fin = new FileInputStream(indexFile);
		FileChannel fc = fin.getChannel();
		fc.position(1025l);// root block starts from 1025th byte as first 1024 bytes reserved for storing metadata
		ObjectInputStream ois = new ObjectInputStream(fin);
		BPlusTree root = (BPlusTree) ois.readObject();
		ois.close();
		
		return root;
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException{
		
		root = new BPlusTree();
		//-create CS6360Asg5TestData.txt cs6360.idx 15
		if (args[0].equalsIgnoreCase("-create")) {
			int keyLength = Integer.parseInt(args[3]);
			String inputFile = args[1];
			String indexFile = args[2];
			createIndex(inputFile, indexFile, keyLength);
			//System.out.println("Index created successfully");
		}
		
		else if (args[0].equalsIgnoreCase("-list")) {
			int numOfRecords = Integer.parseInt(args[3]);
			String indexFile = args[1];
			String key = args[2];
			
			listRecords(indexFile, key, numOfRecords,getRoot(indexFile),false);
			//System.out.println("Listed records successfully");
		}
		
		else if (args[0].equalsIgnoreCase("-find")) {
			String indexFile = args[1];
			String key = args[2];
			
			listRecords(indexFile, key, 1,getRoot(indexFile),false);
		}
		
		else if (args[0].equalsIgnoreCase("-insert")) {
			String indexFile = args[1];
			String data = args[2];
			
			insertNewData(indexFile, data, getRoot(indexFile));
		}
		
	}

}
