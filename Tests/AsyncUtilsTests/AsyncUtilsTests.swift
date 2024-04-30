//
//  AsyncUtilsTests.swift
//  AsyncUtils
//
//  Created by Matteo Ludwig on 24.04.24.
//  Licensed under the MIT-License included in the project.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

import XCTest
@testable import AsyncUtils


actor TestingStorage {
    var starts: [Int: Date] = [:]
    var ends: [Int: Date] = [:]
    
    var counter: Int = 0
    
    var data: ([Int: Date], [Int: Date], Int) {
        (self.starts, self.ends, self.counter)
    }
    
    func incrementCounter() {
        self.counter += 1
    }
    
    func started(_ id: Int) {
        self.starts[id] = Date()
    }
    
    func ended(_ id: Int) {
        self.ends[id] = Date()
    }
}
