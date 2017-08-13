package topk.bean;

public class SumWord implements Comparable<SumWord>{
	
	private int count;
	
	private String word;

	
	public SumWord(int count, String word) {
		this.count = count;
		this.word = word;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	@Override
	public String toString() {
		return word+","+count;
	}

	@Override
	public int compareTo(SumWord word) {
		if (word ==null) {
			return 1;
		}
		return count>word.count? 1:(count<word.count?-1:0);
	}
	
	
	
}
