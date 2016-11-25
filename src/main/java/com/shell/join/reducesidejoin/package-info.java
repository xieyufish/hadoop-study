/**
 * 在reduce端执行join.
 * 完成的任务: 
 * 		共有三个文件,文件内容分别为:
 * 		1.UserDetails
 * 			9901972231,RevanthReddy
			9986570643,Kumar
			9980873232,Anjali
			9964472219,Anusha
		2.DeliveryDetails
			9901972231,001
			9986570643,002
			9980873232,003
			9964472219,004
			9980874545,001
			9664433221,002
		3.DeliveryStatusCodes
			001,Delivered
			002,Pending
			003,Failed
			004,Resend
			
		根据这三个文件, 我想要得到这样子的结果:
			RevanthReddy    Delivered
			Kumar    Pending  
			Anjali    Failed   
			Anusha    Resend   
		本包完成的就是上面所描述的任务, 将DeliveryStatusCodes作为DistributedCache文件,
		分别将UserDetails和DeliveryDetails文件作为输入文件,映射到两个Mapper处理类,这里可以学到MultipleInputs(处理多个不同路径下的文件,需要被不同mapper处理的情况)的运用,
		这里两个mapper类可以跟log包下的GenericWritable这个类联合使用, 不同路径的文件映射到不同mapper再输出不同的类型值, 完成比较复杂的功能,perfect.
		
		本来, 我看到这个例子时的思路是这样的, 就是将DeliveryDetails文件作为DistributedCache文件,将其他两个文件作为mapper输入文件
		我为什么会这样想呢?因为DeliveryDetails文件中电话号码涉及了UserDetails文件,deliverycode涉及了DeliveryStatusCode文件,
		将它作为Cache文件,直接根据这两个值就可以从另两个文件取得对应的值输出.
		但是,我忽略了一个问题, 那就是DeliveryDetails文件的大小,这个文件的容量毫无疑问跟UserDetails文件大小使相同级别的,不适合作为cache文件
		而DeliveryStatusCodes文件是个固定大小的文件且非常小,十分适合作为cache文件.
 */
/**
 * @author Administrator
 *
 */
package com.shell.join.reducesidejoin;