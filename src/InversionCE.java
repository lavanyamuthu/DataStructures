import java.util.Arrays;

public class InversionCE {
	public static int CountInv(int[] array, int start, int end, int invcount) {
		if(end==start) {
			return 0;
		}
		int x=0,y=0,z=0,inv=0;
		int mid = (start+end)/2;
		int[] left = Arrays.copyOfRange(array, 0, mid);
		int[] right = Arrays.copyOfRange(array, mid, array.length);
		x += CountInv(left, start, mid, invcount);
		y += CountInv(right, mid+1, end, invcount);
		z += CountSplitInv(array,left,right);
		return x+y+z;
	}
	public static int CountSplitInv(int[] array, int[] left,int[] right) {
		int i=0,j=0,k=0,count=0;
		while (i< left.length && j < right.length) {
			if(left[i] <= right[j]) {
				array[k] = left[i];
				k++;
				i++;
			}
			else if(left[i] > right[j]) {
				array[k] = right[j];
				k++;
				j++;
				count += left.length-i;
			}
		}
		System.out.println("count is:"+count);
		return count;
	}
	public static void main(String[] args) {
		int[] test = {6,2,3,5,1,7};
		int count=0;
		System.out.println("Inversions is:"+CountInv(test,0,test.length-1,count));
	}
}