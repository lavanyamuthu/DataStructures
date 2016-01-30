import java.util.Arrays;

public class MergeSort {
	public static int[] mergeSort(int[] array) {
		if(array.length <=1)
			return array;
		int mid = (array.length)/2;
		System.out.println("mid:"+mid);
		int[] left = Arrays.copyOfRange(array, 0, mid);
		int[] right = Arrays.copyOfRange(array, mid, array.length);
		System.out.println("left array");
		for(int x=0;x<left.length;x++) {
			System.out.print(left[x]+",");
		}
		System.out.println();
		System.out.println("right array");
		for(int x=0;x<right.length;x++) {
			System.out.print(right[x]+",");
		}
		left = mergeSort(left);
		right = mergeSort(right);
		merge(left,right,array);
		return array;
	}
	public static void merge(int[] left, int[] right, int[] result) {
		int i=0,j=0,k=0;
		while(i<left.length && j < right.length) {
			if(left[i] <= right[j]) {
				result[k] = left[i];
				i++;
			}
			if(left[i] > right[j]) {
				result[k] = right[j];
				j++;
			}
			k++;
		}
		while( i < left.length) {
			result[k] = left[i];
			k++;
			i++;
		}
		while( j < right.length) {
			result[k] = right[j];
			k++;
			j++;
		}
	}
	public static void main(String[] args) {
		int[] test = {6,5,4,3,2,1};
		int[] result = mergeSort(test);
		for(int i=0;i<result.length;i++) {
			System.out.print(result[i]+",");
		}
	}
}