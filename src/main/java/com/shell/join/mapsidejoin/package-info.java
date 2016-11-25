/**
 * 在map端完成join操作.
 * 本包完成的任务: 完成员工信息和部门信息的join操作,将员工信息及其所属部门的名字输出
 * 在这个练习中,要十分注意DistributedCache这个特色功能的使用,
 * 尤其是在mapper中读取cache文件时,要使用getLocalCacheFiles()这个方法, 而不能使用getCacheFiles()这个方法,否则将不能找到文件
 * 这个问题在hadoop官网给的例子中是通过getCacheFiles()方法来取文件,那是不对的.
 */
/**
 * @author Administrator
 *
 */
package com.shell.join.mapsidejoin;