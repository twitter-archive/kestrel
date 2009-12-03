package net.lag.kestrel


import java.io.IOException

/**
 * User: mlagutko
 * Date: Dec 3, 2009
 * Time: 7:29:34 PM
 */

case class BrokenItemException(lastValisPosition: Long, cause: Throwable) extends IOException(cause){

}