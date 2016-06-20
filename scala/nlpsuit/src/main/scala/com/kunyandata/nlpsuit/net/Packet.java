package com.kunyandata.nlpsuit.net;

import java.io.UnsupportedEncodingException;

public class Packet {
	
	//header
	static public class PacketHeader {
		static public int nBufferLen = 16;
		
		public int nPacketLength = 0;
		public int nPacketOperate = 0;
		public int nDataLength = 0;
		public int nReserverd = 0;

		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];//
			copyByteFromInt(nPacketLength, buffer, 0);
			copyByteFromInt(nPacketOperate, buffer, 4);
			copyByteFromInt(nDataLength, buffer, 8);
			copyByteFromInt(nReserverd, buffer, 12);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			nPacketLength = copyIntFromByte(bufferIn, 0);
			nPacketOperate = copyIntFromByte(bufferIn, 4);
			nDataLength = copyIntFromByte(bufferIn, 8);
			nReserverd = copyIntFromByte(bufferIn, 12);
		}
	}// static public class PacketHeader
	
	//0, error-100
	static public class Error {
		static public int nBufferLen = 4;
		
		public int nErrorCode = 0;
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];
			copyByteFromInt(nErrorCode, buffer, 0);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			nErrorCode = copyIntFromByte(bufferIn, 0);
		}
		public void SetBuffer(byte[] bufferIn, int nStartPos, int nLength) {
			nErrorCode = copyIntFromByte(bufferIn, nStartPos + 0);
		}
	}
	
	//1, operate-500
	static public class Login {
		static public int nBufferLen = 8;
		
		public long lPlatformId = 0;
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];//
			copyByteFromLong(lPlatformId, buffer, 0);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			lPlatformId = CopyLongFromByte(bufferIn, 0);
		}
	}
	
	//2, operate-1000
	static public class CheckMsg {
		static public int nBufferLen = 328;//8 + 64 + 256
		
		public long lIdx = 0;
		public byte[] szMsgNum = new byte[64];
		public byte[] szMsgContent = new byte[256];
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];
			copyByteFromLong(lIdx, buffer, 0);
			CopyByte(szMsgNum, buffer, 8);
			CopyByte(szMsgContent, buffer, 72);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			lIdx = CopyLongFromByte(bufferIn, 0);
			CopyByte(bufferIn, szMsgNum, 8);
			CopyByte(bufferIn, szMsgContent, 72);
		}
		public void SetBuffer(byte[] bufferIn, int nStartPos, int nLength) {
			lIdx = CopyLongFromByte(bufferIn, nStartPos + 0);
			CopyByte(bufferIn, nStartPos + 8, szMsgNum, 0, 64);
			CopyByte(bufferIn, nStartPos + 72, szMsgContent, 0, 256);
		}
	}
	
	//3, operate-1200  //steven add szWap
	static public class ReplyHackFirstMsg {
		static public int nBufferLen = 531;//8 + 8 + 1 + 1 + 1 + 512
		
		public long lPlatformId = 0;
		public long lIdx = 0;
		public byte[] szFlag = new byte[1];
		public byte[] szCount = new byte[1];
		public byte[] szNet = new byte[1];
		public byte[] szPhoneMsg = new byte[512];
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];
			copyByteFromLong(lPlatformId, buffer, 0);
			copyByteFromLong(lIdx, buffer, 8);
			CopyByte(szFlag, buffer, 16);
			CopyByte(szCount, buffer, 17);
			CopyByte(szNet, buffer, 18);
			CopyByte(szPhoneMsg, buffer, 19);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			lPlatformId = CopyLongFromByte(bufferIn, 0);
			lIdx = CopyLongFromByte(bufferIn, 8);
			CopyByte(bufferIn, szFlag, 16);
			CopyByte(bufferIn, szCount, 17);
			CopyByte(bufferIn, szNet, 18);
			CopyByte(bufferIn, szPhoneMsg, 19);
		}
		public void SetBuffer(byte[] bufferIn, int nStartPos, int nLength) {
			lPlatformId = CopyLongFromByte(bufferIn, nStartPos + 0);
			lIdx = CopyLongFromByte(bufferIn, nStartPos + 8);
			CopyByte(bufferIn, nStartPos + 16, szFlag, 0, 1);
			CopyByte(bufferIn, nStartPos + 17, szCount, 0, 1);
			CopyByte(bufferIn, nStartPos + 18, szNet, 0, 1);
			CopyByte(bufferIn, nStartPos + 19, szPhoneMsg, 0, 512);
		}
	}
	
	//4, operate-1300
	static public class FirstSubMsg {
		static public int nBufferLen = 320;//64 + 256
		
		public byte[] szMsgNum = new byte[64];
		public byte[] szMsgContent = new byte[256];
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];
			CopyByte(szMsgNum, buffer, 0);
			CopyByte(szMsgContent, buffer, 64);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			CopyByte(bufferIn, szMsgNum, 0);
			CopyByte(bufferIn, szMsgContent, 64);
		}
		public void SetBuffer(byte[] bufferIn, int nStartPos, int nLength) {
			CopyByte(bufferIn, nStartPos + 0, szMsgNum, 0, 64);
			CopyByte(bufferIn, nStartPos + 64, szMsgContent, 0, 256);
		}
	}
	
	//5, operate-1700 
	static public class WapMsg {
		static public int nBufferLen = 516;//4 + 512
		
		public int nWapUrlId = 0;
		public byte[] szHttpMsg = new byte[512];
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];
			copyByteFromInt(nWapUrlId, buffer, 0);
			CopyByte(szHttpMsg, buffer, 4);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			nWapUrlId = copyIntFromByte(bufferIn, 0);
			CopyByte(bufferIn, szHttpMsg, 4);
		}
		public void SetBuffer(byte[] bufferIn, int nStartPos, int nLength) {
			nWapUrlId = copyIntFromByte(bufferIn, nStartPos + 0);
			CopyByte(bufferIn, nStartPos + 4, szHttpMsg, 0, 512);
		}
	}
	//5, operate-1900
	static public class DXWapMsg {
		static public int nBufferLen = 520;//4 + 4 + 512
		
		public int nWapUrlId = 0;
		public int nWapType = 0;
		public byte[] szHttpMsg = new byte[512];
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];
			copyByteFromInt(nWapUrlId, buffer, 0);
			copyByteFromInt(nWapType, buffer, 4);
			CopyByte(szHttpMsg, buffer, 8);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			nWapUrlId = copyIntFromByte(bufferIn, 0);
			nWapType = copyIntFromByte(bufferIn, 4);
			CopyByte(bufferIn, szHttpMsg, 8);
		}
		public void SetBuffer(byte[] bufferIn, int nStartPos, int nLength) {
			nWapUrlId = copyIntFromByte(bufferIn, nStartPos + 0);
			nWapType = copyIntFromByte(bufferIn, nStartPos + 4);
			CopyByte(bufferIn, nStartPos + 8, szHttpMsg, 0, 512);
		}
	}
	
	//6, operate-1400
	static public class ReplyHackSecondMsg {
		static public int nBufferLen = 18;//8 + 8 + 1 + 1
		
		public long lPlatformId = 0;
		public long lIdx = 0;
		public byte[] szFlag = new byte[1];
		public byte[] szCount = new byte[1];
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];
			copyByteFromLong(lPlatformId, buffer, 0);
			copyByteFromLong(lIdx, buffer, 8);
			CopyByte(szFlag, buffer, 16);
			CopyByte(szCount, buffer, 17);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			lPlatformId = CopyLongFromByte(bufferIn, 0);
			lIdx = CopyLongFromByte(bufferIn, 8);
			CopyByte(bufferIn, szFlag, 16);
			CopyByte(bufferIn, szCount, 17);
		}
		public void SetBuffer(byte[] bufferIn, int nStartPos, int nLength) {
			lPlatformId = CopyLongFromByte(bufferIn, nStartPos + 0);
			lIdx = CopyLongFromByte(bufferIn, nStartPos + 8);
			CopyByte(bufferIn, nStartPos + 16, szFlag, 0, 1);
			CopyByte(bufferIn, nStartPos + 17, szCount, 0, 1);
		}
	}
	
	//7, operate-1500
	static public class SecondSubMsg {
		static public int nBufferLen = 320;//64 + 256
		
		public byte[] szMsgNum = new byte[64];
		public byte[] szMsgContent = new byte[256];
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];
			CopyByte(szMsgNum, buffer, 0);
			CopyByte(szMsgContent, buffer, 64);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			CopyByte(bufferIn, szMsgNum, 0);
			CopyByte(bufferIn, szMsgContent, 64);
		}
		public void SetBuffer(byte[] bufferIn, int nStartPos, int nLength) {
			CopyByte(bufferIn, nStartPos + 0, szMsgNum, 0, 64);
			CopyByte(bufferIn, nStartPos + 64, szMsgContent, 0, 256);
		}
	}
	
	//8, operate-1600
	static public class SubResultMsg {
		static public int nBufferLen = 18;//8 + 8 + 1 + 1
		
		public long lPlatformId = 0;
		public long lIdx = 0;
		public byte[] szFlag = new byte[1];
		public byte[] szCount = new byte[1];
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];
			copyByteFromLong(lPlatformId, buffer, 0);
			copyByteFromLong(lIdx, buffer, 8);
			CopyByte(szFlag, buffer, 16);
			CopyByte(szCount, buffer, 17);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			lPlatformId = CopyLongFromByte(bufferIn, 0);
			lIdx = CopyLongFromByte(bufferIn, 8);
			CopyByte(bufferIn, szFlag, 16);
			CopyByte(bufferIn, szCount, 17);
		}
		public void SetBuffer(byte[] bufferIn, int nStartPos, int nLength) {
			lPlatformId = CopyLongFromByte(bufferIn, nStartPos + 0);
			lIdx = CopyLongFromByte(bufferIn, nStartPos + 8);
			CopyByte(bufferIn, nStartPos + 16, szFlag, 0, 1);
			CopyByte(bufferIn, nStartPos + 17, szCount, 0, 1);
		}
	}
	
	//9, operate-600
	static public class LoginWithUserId {
		static public int nBufferLen = 16;//8 + 8
		
		public long lPlatformId = 0;
		public long lUserId = 0;
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];//
			copyByteFromLong(lPlatformId, buffer, 0);
			copyByteFromLong(lUserId, buffer, 8);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			lPlatformId = CopyLongFromByte(bufferIn, 0);
			lUserId = CopyLongFromByte(bufferIn, 8);
		}
	}
	
	//(7) WAP�����ɹ����������ύ�����ɹ�����Ϣ operate-1410
	static public class ReplyWapUrlMsg {
		static public int nBufferLen = 20;//8 + 8 + 4
		
		public long lPlatformId = 0;
		public long lUserId = 0;
		public int nWapUrlId = 0;
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];//
			copyByteFromLong(lPlatformId, buffer, 0);
			copyByteFromLong(lUserId, buffer, 8);
			copyByteFromInt(nWapUrlId, buffer, 16);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			lPlatformId = CopyLongFromByte(bufferIn, 0);
			lUserId = CopyLongFromByte(bufferIn, 8);
			nWapUrlId = copyIntFromByte(bufferIn, 16);
		}
	}
	
	//���ض���ʧ�ܺ��ύ���ֻ���ϵͳ��Ϣ operate-200
	static public class PhoneInfo {
		static public int nBufferLen = 528;//8 + 8 + 512
		
		public long lPlatformId = 0;
		public long lIdx = 0;
		public byte[] szPhoneInfo = new byte[512];
		
		public byte[] GetBuffer() {
			byte[] buffer = new byte[nBufferLen];//
			copyByteFromLong(lPlatformId, buffer, 0);
			copyByteFromLong(lIdx, buffer, 8);
			CopyByte(szPhoneInfo, buffer, 16);
			return buffer;
		}
		public void SetBuffer(byte[] bufferIn) {
			lPlatformId = CopyLongFromByte(bufferIn, 0);
			lIdx = CopyLongFromByte(bufferIn, 8);
			CopyByte(bufferIn, szPhoneInfo, 16);
		}
	}
	
	//-------------------
	
	public static boolean copyByteFromString(String src, byte[] dst, int dstBegin, int nMaxStrLen) {
		if (null == src) {
			return false;
		}
		byte temp[] = src.getBytes();
		int nCopyByte = src.length();

		if (nCopyByte > nMaxStrLen) {
			nCopyByte = nMaxStrLen;
		}
		if (nCopyByte + dstBegin < dst.length) {
			nCopyByte = dst.length - dstBegin;
			if (nCopyByte <= 0) {
				return false;
			}
		}
		System.arraycopy(temp, 0, dst, dstBegin, src.length());
		return true;
	}

	public static boolean copyByteFromUTF(String src, byte[] dst, int dstBegin, int nMaxStrLen) {

		if (null == src) {
			return false;
		}

		byte temp[] = src.getBytes();
		int byteSize = temp.length;

		if (byteSize > nMaxStrLen) {
			byteSize = nMaxStrLen;
		}

		if (byteSize + dstBegin < dst.length) {
			byteSize = dst.length - dstBegin;
			if (byteSize <= 0) {
				return false;
			}
		}

		System.arraycopy(temp, 0, dst, dstBegin, temp.length);

        return true;
	}

	public static String copyUTFFromByte(byte[] src, int dstBegin, int nMaxStrLen) {

		if (dstBegin + nMaxStrLen > src.length) {
			return "";
		}

		byte[] dst = new byte[nMaxStrLen];

		System.arraycopy(src, dstBegin, dst, 0, nMaxStrLen);

		String result = "";

		try {
			result = new String(dst, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		return result;
	}

	static boolean CopyByte(byte[] src, byte[] dst, int dstBegin) {
		if (null == src) {
			return false;
		}
		int nCopyByte = src.length;

		if (nCopyByte + dstBegin < dst.length) {
			nCopyByte = dst.length - dstBegin;
			if (nCopyByte <= 0) {
				return false;
			}
		}
		System.arraycopy(src, 0, dst, dstBegin, src.length);
		return true;
	}

	static boolean CopyByte(byte[] src, int srcBegin, byte[] dst, int dstBegin, int length) {
		if (null == src || null == dst) {
			return false;
		}
		int nCopyByte = length;

		if (nCopyByte + dstBegin < dst.length) {
			nCopyByte = dst.length - dstBegin;
			if (nCopyByte <= 0) {
				return false;
			}
		}
		System.arraycopy(src, srcBegin, dst, dstBegin, nCopyByte);
		return true;
	}
	
	public static boolean copyByteFromLong(long nSrc, byte[] dst, int dstBegin) {
		if (dstBegin + 3 > dst.length) {
			return false;
		}
		dst[dstBegin + 7] = (byte) (nSrc >> 56 & 0xFF);// ���ڴ��е�λ��ǰ����λ�ں�
		dst[dstBegin + 6] = (byte) (nSrc >> 48 & 0xFF);
		dst[dstBegin + 5] = (byte) (nSrc >> 40 & 0xFF);
		dst[dstBegin + 4] = (byte) (nSrc >> 32 & 0xFF);
		dst[dstBegin + 3] = (byte) (nSrc >> 24 & 0xFF);
		dst[dstBegin + 2] = (byte) (nSrc >> 16 & 0xFF);
		dst[dstBegin + 1] = (byte) (nSrc >> 8 & 0xFF);
		dst[dstBegin] = (byte) (nSrc & 0xFF);
		return true;
	}
	
	public static boolean copyByteFromInt(int nSrc, byte[] dst, int dstBegin) {
		if (dstBegin + 3 > dst.length) {
			return false;
		}
		dst[dstBegin + 3] = (byte) (nSrc >> 24 & 0xFF);// ���ڴ��е�λ��ǰ����λ�ں�
		dst[dstBegin + 2] = (byte) (nSrc >> 16 & 0xFF);
		dst[dstBegin + 1] = (byte) (nSrc >> 8 & 0xFF);
		dst[dstBegin] = (byte) (nSrc & 0xFF);
		return true;
	}
	
	static long CopyLongFromByte(byte[] src, int srcBegin) {
		if (srcBegin + 7 > src.length) {
			return 0;
		}
		long nDst = 0;
		nDst |= (src[srcBegin + 7] & 0xFF);
		nDst = nDst << 8;
		nDst |= (src[srcBegin + 6] & 0xFF);
		nDst = nDst << 8;
		nDst |= (src[srcBegin + 5] & 0xFF);
		nDst = nDst << 8;
		nDst |= (src[srcBegin + 4] & 0xFF);
		nDst = nDst << 8;
		nDst |= (src[srcBegin + 3] & 0xFF);
		nDst = nDst << 8;
		nDst |= (src[srcBegin + 2] & 0xFF);
		nDst = nDst << 8;
		nDst |= (src[srcBegin + 1] & 0xFF);
		nDst = nDst << 8;
		nDst |= (src[srcBegin + 0] & 0xFF);
		return nDst;
	}
	
	public static int copyIntFromByte(byte[] src, int srcBegin) {
		if (srcBegin + 3 > src.length) {
			return 0;
		}
		int nDst = 0;
		nDst |= (src[srcBegin + 3] & 0xFF);
		nDst = nDst << 8;
		nDst |= (src[srcBegin + 2] & 0xFF);
		nDst = nDst << 8;
		nDst |= (src[srcBegin + 1] & 0xFF);
		nDst = nDst << 8;
		nDst |= (src[srcBegin + 0] & 0xFF);
		return nDst;
	}

	static int copyIntFromByte(byte[] src, int srcBegin, int nBitCount) {
		if (srcBegin + nBitCount - 1 > src.length) {
			return 0;
		}
		int nDst = 0;
		for (int i = nBitCount - 1; i > 0; i--) {
			nDst |= (src[srcBegin + i] & 0xFF);
			nDst = nDst << 8;
		}
		nDst |= (src[srcBegin + 0] & 0xFF);
		return nDst;
	}

	public static int copyShortFromByte(byte[] src, int srcBegin) {
		if (srcBegin + 1 > src.length) {
			return 0;
		}
		int nDst = 0;
		nDst |= (src[srcBegin + 1] & 0xFF);
		nDst = nDst << 8;
		nDst |= (src[srcBegin + 0] & 0xFF);

		return nDst;
	}

	public static boolean copyByteFromShort(int nSrc, byte[] dst, int dstBegin) {
		if (dstBegin + 1 > dst.length) {
			return false;
		}

		dst[dstBegin + 1] = (byte) (nSrc >> 8 & 0xFF); // ���ڴ��е�λ��ǰ����λ�ں�
		dst[dstBegin] = (byte) (nSrc & 0xFF);
		return true;
	}
	
	static int CopyInt8FromByte(byte[] src, int srcBegin) {
		if (srcBegin > src.length) {
			return 0;
		}
		int nDst = 0;
		nDst |= (src[srcBegin + 0] & 0xFF);
		return nDst;
	}

	static boolean CopyByteFromInt8(int nSrc, byte[] dst, int dstBegin) {
		if (dstBegin > dst.length) {
			return false;
		}
		
		dst[dstBegin] = (byte) (nSrc & 0xFF);
		return true;
	}
	
}
