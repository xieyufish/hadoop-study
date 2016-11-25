/**
 * 学习GenericWritable的用法, 当存在这样的一种场景,map任务的输出时一个key需要对应有多个不同类型的value输出时,可以使用实现GenericWritable的方式
 * 根据GenericWritable的文档说明,还有一个ObjectWritable也是可以使用在这种场景的,但是ObjectWritable会在输出中拼接额外的类声明信息
 */
/**
 * @author Administrator
 *
 */
package com.shell.log.example3;