//
//  TaskExtensionsTests.swift
//  
//
//  Created by Matteo Ludwig on 06.05.24.
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

final class TaskExtensionsTests: XCTestCase {

    var store = TestingStorage()
    
    override func setUpWithError() throws {
        self.store = .init()
    }

    func testSleepFor() async throws {
        let start = Date()
        try await Task.sleep(for: 0.1)
        let delta = Date().timeIntervalSince(start)
        XCTAssertEqual(delta, 0.1, accuracy: 0.01)
    }
    
    func testDelayedTask() async throws {
        let start = Date()
        let task = Task.delayed(by: 0.1) {
            let delta = Date().timeIntervalSince(start)
            XCTAssertEqual(delta, 0.1, accuracy: 0.01)
        }
        try await task.value
        let delta = Date().timeIntervalSince(start)
        XCTAssertEqual(delta, 0.1, accuracy: 0.01)
    }
    
    func testTaskWithTimeout() async throws {
        try await Task.withTimeout(cancelAfter: 0.1) {
            try await Task.sleep(for: 0.08)
            XCTAssertFalse(Task.isCancelled)
            try? await Task.sleep(for: 0.03)
            XCTAssertTrue(Task.isCancelled)
        }
    }
}
